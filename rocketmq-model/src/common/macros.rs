#[macro_export]
macro_rules! hashset {

    () => {
        {
            ::std::collections::HashSet::new()
        }
    };

    ($elem:expr $(, $tail:expr)*) => {
        {
            let mut set = ::std::collections::HashSet::new();
            set.insert($elem);
            $(
                set.insert($tail);
            )*
            set
        }
    };
}
