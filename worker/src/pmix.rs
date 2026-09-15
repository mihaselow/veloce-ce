#[cfg(feature = "pmix")]
pub mod pmix_server {
    use log::{error, info};
    use std::fs;
    use std::path::Path;

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct pmix_proc_t {
        pub nspace: [std::os::raw::c_char; 256],
        pub rank: u32,
    }

    #[link(name = "pmix")]
    extern "C" {
        fn PMIx_server_init(
            module: *mut std::ffi::c_void,
            info: *mut std::ffi::c_void,
            ninfo: usize,
        ) -> i32;
        fn PMIx_server_finalize() -> i32;
        fn PMIx_server_register_nspace(
            nspace: *const std::os::raw::c_char,
            nlocalprocs: i32,
            info: *mut std::ffi::c_void,
            ninfo: usize,
            cbfunc: Option<extern "C" fn(status: i32, cbdata: *mut std::ffi::c_void)>,
            cbdata: *mut std::ffi::c_void,
        ) -> i32;
        fn PMIx_server_register_client(
            proc: *const pmix_proc_t,
            uid: u32,
            gid: u32,
            server_object: *mut std::ffi::c_void,
            cbfunc: Option<extern "C" fn(status: i32, cbdata: *mut std::ffi::c_void)>,
            cbdata: *mut std::ffi::c_void,
        ) -> i32;
    }

    const PMIX_SUCCESS: i32 = 0;

    static G_PMIX_MODULE: [usize; 128] = [0; 128];

    static G_PMIX_SOCKET_PATH: std::sync::OnceLock<String> = std::sync::OnceLock::new();

    fn find_socket_in_dir(dir: &Path) -> Option<std::path::PathBuf> {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(p) = find_socket_in_dir(&path) {
                        return Some(p);
                    }
                } else {
                    #[cfg(target_family = "unix")]
                    {
                        use std::os::unix::fs::FileTypeExt;
                        if let Ok(metadata) = entry.metadata() {
                            if metadata.file_type().is_socket() {
                                return Some(path);
                            }
                        }
                    }
                    let filename = path.file_name()?.to_string_lossy();
                    if filename.contains("pmix") || filename.ends_with(".socket") {
                        return Some(path);
                    }
                }
            }
        }
        None
    }

    pub fn init_global_server() -> Result<(), String> {
        let socket_dir = "/tmp/pmix.veloce";
        let path = Path::new(socket_dir);

        if !path.exists() {
            if let Err(e) = fs::create_dir_all(path) {
                return Err(format!(
                    "Failed to create PMIx socket directory {}: {}",
                    socket_dir, e
                ));
            }
        }

        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o777));
        }

        std::env::set_var("PMIX_SERVER_TMPDIR", socket_dir);

        unsafe {
            // Call the C PMIx library server initialization passing the zeroed module pointer, null and 0
            let status = PMIx_server_init(
                G_PMIX_MODULE.as_ptr() as *mut std::ffi::c_void,
                std::ptr::null_mut(),
                0,
            );
            if status != PMIX_SUCCESS {
                let _ = fs::remove_dir_all(path);
                return Err(format!(
                    "PMIx_server_init failed with status code {}",
                    status
                ));
            }
        }

        // Retry locating the socket file (gives the server time to create it)
        let mut socket_path = None;
        for _ in 0..50 {
            if let Some(p) = find_socket_in_dir(path) {
                socket_path = Some(p.to_string_lossy().to_string());
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let socket_path = match socket_path {
            Some(p) => p,
            None => {
                unsafe {
                    let _ = PMIx_server_finalize();
                }
                let _ = fs::remove_dir_all(path);
                return Err("PMIx server socket file not found after initialization".to_string());
            }
        };

        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::PermissionsExt;
            fn set_permissions_recursive(dir: &Path) {
                let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o777));
                if let Ok(entries) = fs::read_dir(dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o777));
                        if path.is_dir() {
                            set_permissions_recursive(&path);
                        }
                    }
                }
            }
            set_permissions_recursive(path);
        }

        if G_PMIX_SOCKET_PATH.set(socket_path.clone()).is_err() {
            // Already set
        }

        info!(
            "PMIx global server initialized with socket path: {}",
            socket_path
        );
        Ok(())
    }

    pub fn get_server_uri(job_id: u64) -> Option<String> {
        G_PMIX_SOCKET_PATH
            .get()
            .map(|socket_path| format!("pmix-job-{}:unix-socket={}", job_id, socket_path))
    }

    use std::sync::mpsc;

    extern "C" fn op_callback(status: i32, cbdata: *mut std::ffi::c_void) {
        if !cbdata.is_null() {
            unsafe {
                let tx = Box::from_raw(cbdata as *mut mpsc::Sender<i32>);
                let _ = tx.send(status);
            }
        }
    }

    pub fn register_nspace(job_id: u64) -> Result<(), String> {
        let nspace_str = format!("pmix-job-{}", job_id);
        let nspace_cstr = std::ffi::CString::new(nspace_str).map_err(|e| e.to_string())?;

        let (tx, rx) = mpsc::channel::<i32>();
        let tx_boxed = Box::new(tx);
        let cbdata = Box::into_raw(tx_boxed) as *mut std::ffi::c_void;

        unsafe {
            let status = PMIx_server_register_nspace(
                nspace_cstr.as_ptr(),
                1, // Expecting 1 local process spawned by this worker per run_job
                std::ptr::null_mut(),
                0,
                Some(op_callback),
                cbdata,
            );
            if status != PMIX_SUCCESS {
                let _ = Box::from_raw(cbdata as *mut mpsc::Sender<i32>);
                return Err(format!(
                    "PMIx_server_register_nspace failed with status code {}",
                    status
                ));
            }
        }

        match rx.recv() {
            Ok(status) => {
                if status != PMIX_SUCCESS {
                    return Err(format!(
                        "PMIx namespace registration callback returned failure status {}",
                        status
                    ));
                }
            }
            Err(_) => {
                return Err("PMIx namespace registration callback channel disconnected".to_string());
            }
        }

        info!(
            "PMIx namespace registered: pmix-job-{} with local processes limit 1",
            job_id
        );
        Ok(())
    }

    pub fn register_client(job_id: u64, rank: u32, uid: u32, gid: u32) -> Result<(), String> {
        let nspace_str = format!("pmix-job-{}", job_id);
        let nspace_cstr = std::ffi::CString::new(nspace_str).map_err(|e| e.to_string())?;

        let mut proc = pmix_proc_t {
            nspace: [0; 256],
            rank,
        };

        let bytes = nspace_cstr.as_bytes_with_nul();
        if bytes.len() > proc.nspace.len() {
            return Err("Namespace name is too long for pmix_proc_t".to_string());
        }
        for (dest, src) in proc.nspace.iter_mut().zip(bytes.iter()) {
            *dest = *src as std::os::raw::c_char;
        }

        let (tx, rx) = mpsc::channel::<i32>();
        let tx_boxed = Box::new(tx);
        let cbdata = Box::into_raw(tx_boxed) as *mut std::ffi::c_void;

        unsafe {
            let status = PMIx_server_register_client(
                &proc,
                uid,
                gid,
                std::ptr::null_mut(),
                Some(op_callback),
                cbdata,
            );
            if status != PMIX_SUCCESS {
                let _ = Box::from_raw(cbdata as *mut mpsc::Sender<i32>);
                return Err(format!(
                    "PMIx_server_register_client failed for rank {} with status code {}",
                    rank, status
                ));
            }
        }

        match rx.recv() {
            Ok(status) => {
                if status != PMIX_SUCCESS {
                    return Err(format!(
                        "PMIx client registration callback returned failure status {} for rank {}",
                        status, rank
                    ));
                }
            }
            Err(_) => {
                return Err("PMIx client registration callback channel disconnected".to_string());
            }
        }

        info!(
            "PMIx client registered for namespace pmix-job-{} rank {} under uid {} gid {}",
            job_id, rank, uid, gid
        );
        Ok(())
    }

    pub fn finalize_global_server() -> Result<(), String> {
        unsafe {
            let status = PMIx_server_finalize();
            if status != PMIX_SUCCESS {
                error!("PMIx_server_finalize returned non-success code {}", status);
            }
        }

        let socket_dir = "/tmp/pmix.veloce";
        let path = Path::new(socket_dir);
        if path.exists() {
            if let Err(e) = fs::remove_dir_all(path) {
                return Err(format!(
                    "Failed to clean up PMIx socket directory {}: {}",
                    socket_dir, e
                ));
            }
        }

        info!("PMIx global server finalized and cleaned up");
        Ok(())
    }
}
