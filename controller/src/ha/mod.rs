//! High-availability peer networking between controllers.

mod p2p;
mod peer;

pub use p2p::start_p2p_manager;
pub use peer::handle_peer;
