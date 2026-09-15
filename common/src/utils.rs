#[cfg(not(target_arch = "wasm32"))]
pub fn self_restart() -> ! {
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let args: Vec<String> = std::env::args().collect();
    let binary = &args[0];

    log::info!(
        "Initiating self-restart via process replacement: {}",
        binary
    );

    // Command::exec() replaces the current process image with the new one.
    // On Unix, FDs with FD_CLOEXEC set (which is default for tokio/hyper)
    // will be closed automatically.
    let err = Command::new(binary).args(&args[1..]).exec();

    // If exec() returns, it means it failed
    log::error!("Failed to perform self-restart: {}", err);
    std::process::exit(1);
}
