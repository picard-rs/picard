pub mod ask;
pub mod transformation;

#[macro_export]
macro_rules! transform {
    ($actor:expr, $f:expr) => {
        $actor.rec = $f;
    };
}
