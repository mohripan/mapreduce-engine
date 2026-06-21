use crate::{Key, Value};

pub trait Emitter {
    fn emit(&mut self, key: Key, value: Value);
}

#[derive(Debug, Default, Clone)]
pub struct VecEmitter {
    outputs: Vec<(Key, Value)>,
}

impl VecEmitter {
    pub fn new() -> Self {
        Self {
            outputs: Vec::new(),
        }
    }

    pub fn outputs(&self) -> &[(Key, Value)] {
        &self.outputs
    }

    pub fn into_outputs(self) -> Vec<(Key, Value)> {
        self.outputs
    }
}

impl Emitter for VecEmitter {
    fn emit(&mut self, key: Key, value: Value) {
        self.outputs.push((key, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vec_emitter_collects_outputs() {
        let mut emitter = VecEmitter::new();

        emitter.emit("hello".to_string(), "1".to_string());
        emitter.emit("rust".to_string(), "1".to_string());

        assert_eq!(
            emitter.into_outputs(),
            vec![
                ("hello".to_string(), "1".to_string()),
                ("rust".to_string(), "1".to_string()),
            ]
        );
    }
}
