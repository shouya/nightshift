#[derive(Clone, Copy, Debug, Default)]
pub struct RequestInfo {
    pub uid: u32,
    pub gid: u32,
    #[allow(dead_code)]
    pub pid: u32,
}

impl<'a> From<&'a fuser::Request<'a>> for RequestInfo {
    fn from(r: &fuser::Request) -> Self {
        Self {
            uid: r.uid(),
            gid: r.gid(),
            pid: r.pid(),
        }
    }
}
