use common::uuid::Uuid;

#[derive(Debug)]
pub struct MapTask {
    pub start: usize,
    pub end: usize,
    pub id: Uuid
}

impl MapTask {
    pub fn new(start: usize, end: usize) -> Self {
        Self{
            start,
            end,
            id: Uuid::new_v4()
        }
    }
}