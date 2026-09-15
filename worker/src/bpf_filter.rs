use anyhow::Result;
use std::path::Path;

#[cfg(target_os = "linux")]
use anyhow::Context;

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct bpf_insn {
    pub code: u8,
    pub dst_reg: u8, // lower 4 bits dst, upper 4 bits src
    pub off: i16,
    pub imm: i32,
}

#[cfg(target_os = "linux")]
fn insn(code: u8, dst: u8, src: u8, off: i16, imm: i32) -> bpf_insn {
    bpf_insn {
        code,
        dst_reg: dst | (src << 4),
        off,
        imm,
    }
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct ProgLoadAttr {
    prog_type: u32,
    insn_cnt: u32,
    insns: u64,
    license: u64,
    log_level: u32,
    log_size: u32,
    log_buf: u64,
    kern_version: u32,
    prog_flags: u32,
    prog_name: [u8; 16],
    prog_ifindex: u32,
    expected_attach_type: u32,
    prog_btf_fd: u32,
    func_info_rec_size: u32,
    func_info: u64,
    func_info_cnt: u32,
    line_info_rec_size: u32,
    line_info: u64,
    line_info_cnt: u32,
    attach_btf_id: u32,
    attach_prog_fd: u32,
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct ProgAttachAttr {
    target_fd: u32,
    attach_bpf_fd: u32,
    attach_type: u32,
    attach_flags: u32,
    replace_bpf_fd: u32,
}

#[cfg(target_os = "linux")]
const BPF_PROG_LOAD: u32 = 5;
#[cfg(target_os = "linux")]
const BPF_PROG_ATTACH: u32 = 8;
#[cfg(target_os = "linux")]
const BPF_PROG_TYPE_CGROUP_DEVICE: u32 = 15;
#[cfg(target_os = "linux")]
const BPF_CGROUP_DEVICE: u32 = 6;

#[cfg(target_os = "linux")]
fn get_uvm_major() -> Option<u32> {
    if let Ok(content) = std::fs::read_to_string("/proc/devices") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 && parts[1] == "nvidia-uvm" {
                if let Ok(major) = parts[0].parse::<u32>() {
                    return Some(major);
                }
            }
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn compile_bpf_program(gpu_ids: &[u32]) -> Vec<bpf_insn> {
    let mut insns = Vec::new();

    // 0: Load major into R2 (ctx->major at offset 4 of struct bpf_cgroup_dev_ctx)
    insns.push(insn(0x61, 2, 1, 4, 0));
    // 1: Load minor into R3 (ctx->minor at offset 8 of struct bpf_cgroup_dev_ctx)
    insns.push(insn(0x61, 3, 1, 8, 0));

    let mut jumps_to_allow = Vec::new();

    // Allow standard system devices:
    // Major 1: null, zero, random, urandom
    jumps_to_allow.push(insns.len());
    insns.push(insn(0x15, 2, 0, 0, 1)); // JEQ R2, 1, offset

    // Major 5: console, tty, ptmx
    jumps_to_allow.push(insns.len());
    insns.push(insn(0x15, 2, 0, 0, 5)); // JEQ R2, 5, offset

    // Major 136: pts
    jumps_to_allow.push(insns.len());
    insns.push(insn(0x15, 2, 0, 0, 136)); // JEQ R2, 136, offset

    // Allow nvidia-uvm (Unified Memory) major dynamically
    if let Some(uvm_major) = get_uvm_major() {
        jumps_to_allow.push(insns.len());
        insns.push(insn(0x15, 2, 0, 0, uvm_major as i32)); // JEQ R2, uvm_major, offset
    }

    // Default allow for any non-Nvidia GPU driver access
    // JNE R2, 195, offset -> if not Nvidia (195), allow
    jumps_to_allow.push(insns.len());
    insns.push(insn(0x55, 2, 0, 0, 195));

    // NVIDIA major is matched. Check Nvidia minors:
    // Allow nvidiactl (255)
    jumps_to_allow.push(insns.len());
    insns.push(insn(0x15, 3, 0, 0, 255)); // JEQ R3, 255, offset

    // Allow nvidia-modeset (254)
    jumps_to_allow.push(insns.len());
    insns.push(insn(0x15, 3, 0, 0, 254)); // JEQ R3, 254, offset

    // Allow specific assigned GPU IDs
    for &gpu_id in gpu_ids {
        jumps_to_allow.push(insns.len());
        insns.push(insn(0x15, 3, 0, 0, gpu_id as i32)); // JEQ R3, gpu_id, offset
    }

    // DENY Block (R0 = 0, Exit)
    insns.push(insn(0xb7, 0, 0, 0, 0)); // MOV64 R0, 0
    insns.push(insn(0x95, 0, 0, 0, 0)); // EXIT

    // ALLOW Block (R0 = 1, Exit)
    let allow_idx = insns.len();
    insns.push(insn(0xb7, 0, 0, 0, 1)); // MOV64 R0, 1
    insns.push(insn(0x95, 0, 0, 0, 0)); // EXIT

    // Pass 2: Patch offsets
    for idx in jumps_to_allow {
        let offset = (allow_idx as i16) - (idx as i16) - 1;
        insns[idx].off = offset;
    }

    insns
}

#[cfg(target_os = "linux")]
fn load_bpf_program(insns: &[bpf_insn]) -> Result<i32> {
    let license = b"GPL\0";
    let mut attr = ProgLoadAttr {
        prog_type: BPF_PROG_TYPE_CGROUP_DEVICE,
        insn_cnt: insns.len() as u32,
        insns: insns.as_ptr() as u64,
        license: license.as_ptr() as u64,
        log_level: 0,
        log_size: 0,
        log_buf: 0,
        kern_version: 0,
        prog_flags: 0,
        prog_name: [0; 16],
        prog_ifindex: 0,
        expected_attach_type: 0,
        prog_btf_fd: 0,
        func_info_rec_size: 0,
        func_info: 0,
        func_info_cnt: 0,
        line_info_rec_size: 0,
        line_info: 0,
        line_info_cnt: 0,
        attach_btf_id: 0,
        attach_prog_fd: 0,
    };

    let prog_name_str = b"veloce_gpu_dev\0";
    let len = prog_name_str.len().min(16);
    attr.prog_name[..len].copy_from_slice(&prog_name_str[..len]);

    let size = std::mem::size_of::<ProgLoadAttr>() as u32;
    let res = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_PROG_LOAD,
            &attr as *const _ as *const std::ffi::c_void,
            size,
        )
    };

    if res < 0 {
        let err = std::io::Error::last_os_error();
        return Err(anyhow::anyhow!("Failed to load BPF program: {}", err));
    }

    Ok(res as i32)
}

#[cfg(target_os = "linux")]
fn attach_bpf_to_cgroup(cgroup_path: &Path, prog_fd: i32) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    let file = std::fs::File::open(cgroup_path)
        .context("Failed to open cgroup directory for attachment")?;
    let target_fd = file.as_raw_fd();

    let attr = ProgAttachAttr {
        target_fd: target_fd as u32,
        attach_bpf_fd: prog_fd as u32,
        attach_type: BPF_CGROUP_DEVICE,
        attach_flags: 0,
        replace_bpf_fd: 0,
    };

    let size = std::mem::size_of::<ProgAttachAttr>() as u32;
    let res = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_PROG_ATTACH,
            &attr as *const _ as *const std::ffi::c_void,
            size,
        )
    };

    if res < 0 {
        let err = std::io::Error::last_os_error();
        return Err(anyhow::anyhow!(
            "Failed to attach BPF program to cgroup: {}",
            err
        ));
    }

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn attach_gpu_filter(cgroup_path: &Path, gpu_ids: &[u32]) -> Result<()> {
    let insns = compile_bpf_program(gpu_ids);
    let prog_fd = load_bpf_program(&insns)?;
    attach_bpf_to_cgroup(cgroup_path, prog_fd)?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn attach_gpu_filter(_cgroup_path: &Path, _gpu_ids: &[u32]) -> Result<()> {
    anyhow::bail!("BPF cgroup device filtering is only supported on Linux")
}
