#![cfg(not(target_arch = "wasm32"))]

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use bytes::{Buf, BufMut, BytesMut};
use log::warn;
use snow::{Builder, TransportState};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

// Max message size for Noise protocol (65535 is strict limit, we use slightly less)
const MAX_NOISE_MESSAGE_LEN: usize = 65535;
// Length field size (u16)
const LENGTH_FIELD_SIZE: usize = 2;

static PATTERN: &str = "Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s";

/// A wrapper around an underlying stream that handles Noise protocol encryption/decryption.
pub struct NoiseStream<S> {
    inner: S,
    noise: TransportState,

    // Buffer for accumulating incoming encrypted data (header + body)
    read_buffer: BytesMut,
    // Buffer for storing decrypted data that hasn't been read by the user yet
    decrypted_buffer: BytesMut,
    // State for reading length
    reading_length: bool,
    expected_len: usize,

    // Buffer for outgoing encrypted data to handle partial writes
    output_buffer: BytesMut,
}

impl<S> NoiseStream<S> {
    pub fn new(inner: S, noise: TransportState) -> Self {
        Self {
            inner,
            noise,
            read_buffer: BytesMut::with_capacity(MAX_NOISE_MESSAGE_LEN + LENGTH_FIELD_SIZE),
            decrypted_buffer: BytesMut::with_capacity(MAX_NOISE_MESSAGE_LEN),
            reading_length: true,
            expected_len: LENGTH_FIELD_SIZE,
            output_buffer: BytesMut::with_capacity(MAX_NOISE_MESSAGE_LEN + LENGTH_FIELD_SIZE),
        }
    }

    pub fn get_remote_static(&self) -> Option<&[u8]> {
        self.noise.get_remote_static()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for NoiseStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // 1. If we have decrypted data, return it immediately
        if this.decrypted_buffer.remaining() > 0 {
            let len = std::cmp::min(buf.remaining(), this.decrypted_buffer.remaining());
            buf.put_slice(&this.decrypted_buffer[..len]);
            this.decrypted_buffer.advance(len);

            // If empty, reclaim space.
            if this.decrypted_buffer.remaining() == 0 {
                this.decrypted_buffer.clear();
            }
            return Poll::Ready(Ok(()));
        }

        // 2. Read from inner stream until we have a full packet (Length + Body)
        loop {
            // Ensure capacity
            if this.read_buffer.len() < this.expected_len {
                // Using a temporary buffer to read from inner and extend read_buffer
                let mut temp_buf = [0u8; 4096];
                let mut read_buf = ReadBuf::new(&mut temp_buf);

                match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let filled = read_buf.filled();
                        if filled.is_empty() {
                            // EOF
                            if this.read_buffer.is_empty() {
                                return Poll::Ready(Ok(()));
                            } else {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "Incomplete Noise packet",
                                )));
                            }
                        }
                        this.read_buffer.extend_from_slice(filled);
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Check if we have enough for the next stage
            if this.reading_length {
                if this.read_buffer.len() >= LENGTH_FIELD_SIZE {
                    let len_bytes = &this.read_buffer[..LENGTH_FIELD_SIZE];
                    let len = u16::from_be_bytes([len_bytes[0], len_bytes[1]]) as usize;
                    this.read_buffer.advance(LENGTH_FIELD_SIZE);

                    this.reading_length = false;
                    this.expected_len = len; // Now expect the body
                }
            } else {
                if this.read_buffer.len() >= this.expected_len {
                    // Decrypt
                    let chunk = this.read_buffer.split_to(this.expected_len);

                    // Reset for next packet
                    this.reading_length = true;
                    this.expected_len = LENGTH_FIELD_SIZE;

                    // Allocate enough space for plaintext (max is same as ciphertext)
                    let mut plaintext = vec![0u8; chunk.len()];
                    match this.noise.read_message(&chunk, &mut plaintext) {
                        Ok(n) => {
                            this.decrypted_buffer.extend_from_slice(&plaintext[..n]);

                            // Return data to user
                            let len =
                                std::cmp::min(buf.remaining(), this.decrypted_buffer.remaining());
                            buf.put_slice(&this.decrypted_buffer[..len]);
                            this.decrypted_buffer.advance(len);
                            return Poll::Ready(Ok(()));
                        }
                        Err(e) => {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Noise decryption error: {}", e),
                            )))
                        }
                    }
                }
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for NoiseStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // 1. Try to flush existing output buffer
        if !this.output_buffer.is_empty() {
            let written = match Pin::new(&mut this.inner).poll_write(cx, &this.output_buffer) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            this.output_buffer.advance(written);
            if this.output_buffer.is_empty() {
                this.output_buffer.clear();
            } else {
                // Still have data pending, cannot accept new data yet
                return Poll::Pending;
            }
        }

        // 2. Encrypt new data
        // Noise overhead is 16 bytes for Poly1305
        let max_payload = MAX_NOISE_MESSAGE_LEN - 16;
        let len = std::cmp::min(buf.len(), max_payload);

        // We write directly into the output buffer: [Length][Ciphertext]
        let mut ciphertext = vec![0u8; MAX_NOISE_MESSAGE_LEN];
        let n = match this.noise.write_message(&buf[..len], &mut ciphertext) {
            Ok(n) => n,
            Err(e) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Noise encryption error: {}", e),
                )))
            }
        };

        this.output_buffer.put_u16(n as u16);
        this.output_buffer.extend_from_slice(&ciphertext[..n]);

        // 3. Try to write immediately
        let written = match Pin::new(&mut this.inner).poll_write(cx, &this.output_buffer) {
            Poll::Ready(Ok(n)) => n,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => {
                // We accepted the data (buffered it), but couldn't write to inner yet.
                // We return 'len' as accepted bytes.
                return Poll::Ready(Ok(len));
            }
        };

        this.output_buffer.advance(written);
        if this.output_buffer.is_empty() {
            this.output_buffer.clear();
        }

        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while !this.output_buffer.is_empty() {
            let written = match Pin::new(&mut this.inner).poll_write(cx, &this.output_buffer) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            this.output_buffer.advance(written);
        }
        this.output_buffer.clear();
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // We must flush before shutdown
        if !self.output_buffer.is_empty() {
            match self.as_mut().poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

fn derive_psk(secret_key: &str) -> Result<[u8; 32]> {
    // 1. Try to decode as base64 (already 32 bytes)
    if let Ok(v) = general_purpose::STANDARD.decode(secret_key) {
        if v.len() == 32 {
            let mut psk = [0u8; 32];
            psk.copy_from_slice(&v);
            return Ok(psk);
        }
    }

    // 2. Fallback: Key Derivation using SHA-256 for a stable 32-byte key from any string.
    // However, we log a warning if it's too short.
    if secret_key.len() < 16 {
        warn!("Using a short cluster secret is insecure. Please use a strong, high-entropy key.");
    }

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(secret_key.as_bytes());
    let result = hasher.finalize();

    let mut psk = [0u8; 32];
    psk.copy_from_slice(&result);
    Ok(psk)
}

pub async fn upgrade_initiator<S>(
    mut stream: S,
    secret_key: &str, // PSK
) -> Result<NoiseStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let builder = Builder::new(PATTERN.parse()?);
    let psk = derive_psk(secret_key)?;

    let static_key = builder.generate_keypair()?.private;
    let mut noise = builder
        .local_private_key(&static_key)?
        .psk(3, &psk)?
        .build_initiator()?;

    // Handshake Buffer
    let mut buf = vec![0u8; 65535];

    // -> e
    let len = noise.write_message(&[], &mut buf)?;
    write_frame(&mut stream, &buf[..len]).await?;

    // <- e, ee, s, es
    let len = read_frame(&mut stream, &mut buf).await?;
    noise.read_message(&buf[..len], &mut [])?;

    // -> s, se, psk
    let len = noise.write_message(&[], &mut buf)?;
    write_frame(&mut stream, &buf[..len]).await?;

    let noise = noise.into_transport_mode()?;
    Ok(NoiseStream::new(stream, noise))
}

pub async fn upgrade_responder<S>(
    mut stream: S,
    secret_key: &str, // PSK
) -> Result<NoiseStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let builder = Builder::new(PATTERN.parse()?);
    let psk = derive_psk(secret_key)?;

    let static_key = builder.generate_keypair()?.private;
    let mut noise = builder
        .local_private_key(&static_key)?
        .psk(3, &psk)?
        .build_responder()?;

    let mut buf = vec![0u8; 65535];

    // -> e
    let len = read_frame(&mut stream, &mut buf).await?;
    noise.read_message(&buf[..len], &mut [])?;

    // <- e, ee, s, es
    let len = noise.write_message(&[], &mut buf)?;
    write_frame(&mut stream, &buf[..len]).await?;

    // -> s, se, psk
    let len = read_frame(&mut stream, &mut buf).await?;
    noise.read_message(&buf[..len], &mut [])?;

    let noise = noise.into_transport_mode()?;
    Ok(NoiseStream::new(stream, noise))
}

async fn write_frame<S>(stream: &mut S, data: &[u8]) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let len = data.len() as u16;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_frame<S>(stream: &mut S, buf: &mut [u8]) -> Result<usize>
where
    S: AsyncRead + Unpin,
{
    let mut len_bytes = [0u8; 2];
    stream.read_exact(&mut len_bytes).await?;
    let len = u16::from_be_bytes(len_bytes) as usize;

    if len > buf.len() {
        return Err(anyhow!("Frame too large"));
    }

    stream.read_exact(&mut buf[..len]).await?;
    Ok(len)
}
