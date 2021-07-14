use crate::big_int_str::BigIntStr;
use core::ops::{Add, AddAssign, Div, Mul, Sub};
use num_traits::{Pow, Zero};
use serde::{de, Deserialize, Deserializer};
use serde_json::value::RawValue;
use std::str::FromStr;

pub use bigdecimal::BigDecimal;
pub use num_bigint::{BigInt, Sign};
pub use num_rational::BigRational;
pub use paste::paste;

/// Construct a `$name` detailed number that have decimal, fraction and rational representations.
/// The macro takes the `$name` name of the structure and the `$base_name` that is used to generate three different fields:
/// `<$base_name>`- decimal representation
/// `<$base_name>_fraction` - fraction representation
/// `<$base_name>_rat` - rational representation
///
/// Note the constructable `$name` type implements the `From<MmNumber>` trait.
///
/// Example: `construct_detailed(MyVolume, volume)` will construct something like that:
/// ```
/// struct MyVolume {
///     volume: BigDecimal,
///     volume_fraction: Fraction,
///     volume_rat: BigRational,
/// }
/// ```
#[macro_export]
macro_rules! construct_detailed {
    ($name: ident, $base_field: ident) => {
        $crate::mm_number::paste! {
            #[derive(Clone, Debug, Serialize)]
            pub struct $name {
                $base_field: $crate::mm_number::BigDecimal,
                [<$base_field _fraction>]: $crate::mm_number::Fraction,
                [<$base_field _rat>]: $crate::mm_number::BigRational,
            }

            impl From<$crate::mm_number::MmNumber> for $name {
                fn from(mm_num: $crate::mm_number::MmNumber) -> Self {
                    Self {
                        $base_field: mm_num.to_decimal(),
                        [<$base_field _fraction>]: mm_num.to_fraction(),
                        [<$base_field _rat>]: mm_num.to_ratio(),
                    }
                }
            }

            #[allow(dead_code)]
            impl $name {
                pub fn as_ratio(&self) -> &$crate::mm_number::BigRational {
                    &self.[<$base_field _rat>]
                }
            }
        }
    };
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize)]
pub struct MmNumber(BigRational);

/// Rational number representation de/serializable in human readable form
/// Should simplify the visual perception and parsing in code
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Fraction {
    /// Numerator
    numer: BigIntStr,
    /// Denominator
    denom: BigIntStr,
}

impl Fraction {
    /// Numerator
    pub fn numer(&self) -> &BigInt { self.numer.inner() }

    /// Denominator
    pub fn denom(&self) -> &BigInt { self.denom.inner() }
}

impl From<BigRational> for Fraction {
    fn from(ratio: BigRational) -> Fraction {
        let (numer, denom) = ratio.into();
        Fraction {
            numer: numer.into(),
            denom: denom.into(),
        }
    }
}

impl From<Fraction> for BigRational {
    fn from(fraction: Fraction) -> Self { BigRational::new(fraction.numer.into(), fraction.denom.into()) }
}

impl From<BigDecimal> for Fraction {
    fn from(dec: BigDecimal) -> Fraction { from_dec_to_ratio(&dec).into() }
}

impl<'de> Deserialize<'de> for Fraction {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct FractionHelper {
            numer: BigIntStr,
            denom: BigIntStr,
        }

        let maybe_fraction: FractionHelper = Deserialize::deserialize(deserializer)?;
        if maybe_fraction.denom.inner() == &0.into() {
            return Err(de::Error::custom("denom can not be 0"));
        }

        Ok(Fraction {
            numer: maybe_fraction.numer,
            denom: maybe_fraction.denom,
        })
    }
}

pub fn from_ratio_to_dec(r: &BigRational) -> BigDecimal {
    BigDecimal::from(r.numer().clone()) / BigDecimal::from(r.denom().clone())
}

pub fn from_dec_to_ratio(d: &BigDecimal) -> BigRational {
    let (num, scale) = d.as_bigint_and_exponent();
    let ten = BigInt::from(10);
    if scale >= 0 {
        BigRational::new(num, ten.pow(scale as u64))
    } else {
        BigRational::new(num * ten.pow((-scale) as u64), 1.into())
    }
}

/// Handwritten deserialization method allows the MmNumber to be deserialized from:
/// 1. big rational representation,
/// 2. decimal string e.g. "0.1"
/// 3. fraction object e.g. { "numer":"2", "denom":"3" }
/// IMPORTANT: the deserialization implementation works properly from JSON only!
/// Consider using BigRational type directly for other serde implementations
impl<'de> Deserialize<'de> for MmNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let raw: Box<RawValue> = Deserialize::deserialize(deserializer)?;

        if let Ok(dec) = BigDecimal::from_str(&raw.get().trim_matches('"')) {
            return Ok(MmNumber(from_dec_to_ratio(&dec)));
        };

        if let Ok(rat) = serde_json::from_str::<BigRational>(raw.get()) {
            return Ok(MmNumber(rat));
        };

        if let Ok(fraction) = serde_json::from_str::<Fraction>(raw.get()) {
            return Ok(MmNumber(fraction.into()));
        };

        Err(de::Error::custom(format!(
            "Could not deserialize any variant of MmNumber from {}",
            raw.get()
        )))
    }
}

impl std::fmt::Display for MmNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "{}", from_ratio_to_dec(&self.0)) }
}

impl From<BigDecimal> for MmNumber {
    fn from(n: BigDecimal) -> MmNumber { from_dec_to_ratio(&n).into() }
}

impl From<BigRational> for MmNumber {
    fn from(r: BigRational) -> MmNumber { MmNumber(r) }
}

impl From<Fraction> for MmNumber {
    fn from(f: Fraction) -> MmNumber { MmNumber(f.into()) }
}

impl From<MmNumber> for BigDecimal {
    fn from(n: MmNumber) -> BigDecimal { from_ratio_to_dec(&n.0) }
}

impl From<MmNumber> for BigRational {
    fn from(n: MmNumber) -> BigRational { n.0 }
}

impl From<u64> for MmNumber {
    fn from(n: u64) -> MmNumber { BigRational::from_integer(n.into()).into() }
}

impl From<(u64, u64)> for MmNumber {
    fn from(tuple: (u64, u64)) -> MmNumber { BigRational::new(tuple.0.into(), tuple.1.into()).into() }
}

impl Mul for MmNumber {
    type Output = MmNumber;

    fn mul(self, rhs: Self) -> Self::Output { (self.0 * rhs.0).into() }
}

impl Mul for &MmNumber {
    type Output = MmNumber;

    fn mul(self, rhs: Self) -> Self::Output {
        let lhs = &self.0;
        let rhs = &rhs.0;
        MmNumber(lhs * rhs)
    }
}

impl Add for MmNumber {
    type Output = MmNumber;

    fn add(self, rhs: Self) -> Self::Output { (self.0 + rhs.0).into() }
}

impl AddAssign for MmNumber {
    fn add_assign(&mut self, rhs: Self) { self.0 += rhs.0; }
}

impl AddAssign<&MmNumber> for MmNumber {
    fn add_assign(&mut self, rhs: &Self) { self.0 += &rhs.0; }
}

impl Add for &MmNumber {
    type Output = MmNumber;

    fn add(self, rhs: Self) -> Self::Output {
        let lhs = &self.0;
        let rhs = &rhs.0;
        MmNumber(lhs + rhs)
    }
}

impl Sub for MmNumber {
    type Output = MmNumber;

    fn sub(self, rhs: Self) -> Self::Output { (self.0 - rhs.0).into() }
}

impl Sub for &MmNumber {
    type Output = MmNumber;

    fn sub(self, rhs: Self) -> Self::Output { (&self.0 - &rhs.0).into() }
}

impl Div for MmNumber {
    type Output = MmNumber;

    fn div(self, rhs: MmNumber) -> MmNumber { (self.0 / rhs.0).into() }
}

impl Div for &MmNumber {
    type Output = MmNumber;

    fn div(self, rhs: &MmNumber) -> MmNumber {
        let lhs = &self.0;
        let rhs = &rhs.0;
        MmNumber(lhs / rhs)
    }
}

impl PartialOrd<BigDecimal> for MmNumber {
    fn partial_cmp(&self, other: &BigDecimal) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(&from_dec_to_ratio(other)))
    }
}

impl PartialEq<BigDecimal> for MmNumber {
    fn eq(&self, rhs: &BigDecimal) -> bool { self.0 == from_dec_to_ratio(rhs) }
}

impl Default for MmNumber {
    fn default() -> MmNumber { BigRational::from_integer(0.into()).into() }
}

impl MmNumber {
    /// Returns Fraction representation of the number
    pub fn to_fraction(&self) -> Fraction {
        Fraction {
            numer: self.0.numer().clone().into(),
            denom: self.0.denom().clone().into(),
        }
    }

    /// Clones the internal BigRational
    pub fn to_ratio(&self) -> BigRational { self.0.clone() }

    /// Get BigDecimal representation
    pub fn to_decimal(&self) -> BigDecimal { from_ratio_to_dec(&self.0) }

    pub fn numer(&self) -> &BigInt { self.0.numer() }

    pub fn denom(&self) -> &BigInt { self.0.denom() }

    pub fn is_zero(&self) -> bool { self.0.is_zero() }
}

impl From<i32> for MmNumber {
    fn from(num: i32) -> MmNumber { MmNumber(BigRational::from_integer(num.into())) }
}

/// Useful for tests
impl From<&'static str> for MmNumber {
    fn from(str: &'static str) -> MmNumber {
        let num: BigDecimal = str.parse().expect("Input should be string representing decimal num");
        num.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json as json;
    use std::str::FromStr;

    #[test]
    fn test_from_dec_to_ratio() {
        let number: BigDecimal = "11.00000000000000000000000000000000000000".parse().unwrap();
        let rational = from_dec_to_ratio(&number);
        assert_eq!(*rational.numer(), 11.into());
        assert_eq!(*rational.denom(), 1.into());

        let number: BigDecimal = "0.00000001".parse().unwrap();
        let rational = from_dec_to_ratio(&number);
        assert_eq!(*rational.numer(), 1.into());
        assert_eq!(*rational.denom(), 100000000.into());

        let number: BigDecimal = 1.into();
        let rational = from_dec_to_ratio(&number);
        assert_eq!(*rational.numer(), 1.into());
        assert_eq!(*rational.denom(), 1.into());
    }

    #[test]
    fn test_mm_number_deserialize_from_dec() {
        let vals = vec![
            "1.0",
            "0.5",
            "50",
            "1e-3",
            "1e12",
            "0.3333333333333333",
            "3.141592653589793",
            "12.0010",
        ];

        for num in vals {
            let decimal: BigDecimal = BigDecimal::from_str(num).unwrap();
            let expected: MmNumber = from_dec_to_ratio(&decimal).into();
            let actual: MmNumber = json::from_str(&num).unwrap();
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_mm_number_deserialize_from_ratio() {
        let vals: Vec<BigRational> = vec![
            BigRational::from_integer(0.into()),
            BigRational::from_integer(81516161.into()),
            BigRational::new(370.into(), 5123.into()),
            BigRational::new(1742152.into(), 848841.into()),
        ];

        for num in vals {
            let serialized = json::to_string(&num).unwrap();
            let expected: MmNumber = num.into();
            let actual: MmNumber = json::from_str(&serialized).unwrap();
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_mm_number_deserialize() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Helper {
            num: MmNumber,
            nums: Vec<MmNumber>,
        }

        let data = Helper {
            num: BigRational::new(1.into(), 10.into()).into(),
            nums: vec![
                BigRational::from_integer(50.into()).into(),
                BigRational::new(1.into(), 1000.into()).into(),
                BigRational::from_integer(1000000000000i64.into()).into(),
                BigRational::new(33.into(), 100.into()).into(),
                BigRational::new(5.into(), 2.into()).into(),
            ],
        };

        // A JSON input with plenty of whitespace.
        let json = json!({
            "num": "0.1",
            "nums": ["50", "1e-3", "1e12", "0.33", "2.5"]
        });

        assert_eq!(data, json::from_value(json).unwrap());
    }

    #[test]
    fn test_deserialize_fraction() {
        let num_str = r#"{"numer":"2000","denom":"3"}"#;
        let actual: Fraction = json::from_str(num_str).unwrap();
        assert_eq!(&BigInt::from(2000), actual.numer());
        assert_eq!(&BigInt::from(3), actual.denom());

        let num_str = r#"{"numer":"2000","denom":"0"}"#;
        let err = json::from_str::<Fraction>(num_str).unwrap_err();
        let expected_msg = "denom can not be 0";
        assert_eq!(expected_msg, err.to_string());
    }

    #[test]
    fn test_mm_number_deserialize_from_fraction() {
        let num_str = r#"{"numer":"2000","denom":"3"}"#;
        let expected: MmNumber = BigRational::new(2000.into(), 3.into()).into();
        let actual: MmNumber = json::from_str(num_str).unwrap();
        assert_eq!(expected, actual);

        let num_str = r#"{"numer":"2000","denom":"0"}"#;
        json::from_str::<MmNumber>(num_str).unwrap_err();
    }

    #[test]
    fn test_mm_number_to_fraction() {
        let num: MmNumber = MmNumber(BigRational::new(2000.into(), 3.into()));
        let fraction = num.to_fraction();
        assert_eq!(num.0.numer(), fraction.numer());
        assert_eq!(num.0.denom(), fraction.denom());
    }

    #[test]
    fn test_construct_detailed() {
        construct_detailed!(MyNumber, number);

        let mm_num = MmNumber::from((1, 10));
        let actual = MyNumber {
            number: mm_num.to_decimal(),
            number_fraction: mm_num.to_fraction(),
            number_rat: mm_num.to_ratio(),
        };
        let expected = MyNumber::from(mm_num);
        assert_eq!(actual.number, expected.number);
        assert_eq!(actual.number_rat, expected.number_rat);
        // Fraction doesn't implement `PartialEq` trait
    }
}
