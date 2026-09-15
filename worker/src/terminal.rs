use portable_pty::{CommandBuilder, NativePtySystem, PtyPair, PtySize, PtySystem};
use sysinfo::{PidExt, ProcessExt, System, SystemExt};

pub struct ActiveTerminalSession {
    pub writer: Box<dyn std::io::Write + Send>,
    pub pair: PtyPair,
}

fn process_name(sys: &System, pid: u32) -> String {
    sys.process(sysinfo::Pid::from_u32(pid))
        .map(|p| p.name().to_string())
        .unwrap_or_default()
}

/// X11 / desktop helpers that share the container namespaces but are poor nsenter targets
/// (e.g. VNC Apptainer jobs where the lowest-PID child is often Xtigervnc).
fn is_namespace_helper(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("xtigervnc")
        || n.contains("xvnc")
        || n.contains("xorg")
        || n == "x"
        || n.contains("dbus-daemon")
        || n.contains("dbus-launch")
        || n.contains("dbus-broker")
        || n.starts_with("pipewire")
        || n.starts_with("wireplumber")
        || n == "gpg-agent"
        || n == "ssh-agent"
}

fn is_preferred_payload(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("bash")
        || n.contains("zsh")
        || n.contains("fish")
        || n == "sh"
        || n.contains("xfce4-terminal")
        || n.contains("xfce4-session")
        || n.contains("startxfce")
        || n.contains("gnome-session")
        || n.contains("python")
        || n.contains("julia")
        || n.contains("matlab")
        || n == "r"
        || n.contains("rscript")
        || n.contains("java")
        || n.contains("node")
}

/// Pick a process inside the job's namespaces for `nsenter`.
/// Prefer real payload / session processes; never dive into X11 helpers like Xtigervnc.
fn find_attach_pid(root_pid: u32) -> u32 {
    let mut sys = System::new();
    sys.refresh_processes();

    let mut current_pid = root_pid;

    for _ in 0..10 {
        let mut children: Vec<_> = sys
            .processes()
            .values()
            .filter(|p| p.parent().map(|pp| pp.as_u32()) == Some(current_pid))
            .collect();

        if children.is_empty() {
            break;
        }

        children.sort_by_key(|p| p.pid().as_u32());

        let mut non_helpers: Vec<_> = children
            .iter()
            .copied()
            .filter(|p| !is_namespace_helper(p.name()))
            .collect();

        if non_helpers.is_empty() {
            // Only helpers under this node (typical VNC tree). Stay here — same
            // namespaces as Xtigervnc, without attaching to the X server itself.
            break;
        }

        non_helpers.sort_by_key(|p| {
            let preferred = is_preferred_payload(p.name());
            (!preferred, p.pid().as_u32())
        });

        current_pid = non_helpers[0].pid().as_u32();
    }

    log::info!(
        "TTY attach target PID {} ({}) from job root {}",
        current_pid,
        process_name(&sys, current_pid),
        root_pid
    );
    current_pid
}

pub fn spawn_terminal(
    working_directory: &str,
    target_pid: Option<u32>, // Solver root PID for namespace attachment
) -> std::io::Result<ActiveTerminalSession> {
    let pty_system = NativePtySystem::default();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // Set up bash execution context.
    // If a target container PID is provided on Linux, pivot child namespaces programmatically using nsenter.
    let mut cmd = if let Some(pid) = target_pid {
        let actual_pid = find_attach_pid(pid);
        log::info!(
            "TTY attaching via nsenter to PID {} (Job Root: {})",
            actual_pid,
            pid
        );

        let mut c = CommandBuilder::new("/usr/bin/nsenter");
        c.args(&[
            "--target",
            &actual_pid.to_string(),
            "--mount",
            "--uts",
            "--ipc",
            "--net",
            "--pid",
            "/bin/bash",
        ]);
        c
    } else {
        CommandBuilder::new("/bin/bash")
    };

    cmd.cwd(working_directory);
    cmd.env("TERM", "xterm-256color");
    cmd.env("PS1", "veloce-container:\\w\\$ ");

    let _child = pair.slave.spawn_command(cmd).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to spawn interactive shell: {e}"),
        )
    })?;

    let writer = pair.master.take_writer().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to open PTY writer: {e}"),
        )
    })?;

    Ok(ActiveTerminalSession { writer, pair })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_native_terminal_spawn() {
        let session = spawn_terminal("/tmp", None).unwrap();
        drop(session);
    }

    #[test]
    fn test_helper_detection() {
        assert!(is_namespace_helper("Xtigervnc"));
        assert!(is_namespace_helper("dbus-daemon"));
        assert!(!is_namespace_helper("xfce4-terminal"));
        assert!(is_preferred_payload("bash"));
        assert!(is_preferred_payload("xfce4-session"));
    }
}
