use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SsbValue {
    pub author: String,
    pub sequence: u32,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SsbMessage {
    pub key: String,
    pub value: SsbValue,
}
