use anyhow::bail;
use frame_decode::storage::StorageHasher;
use frame_metadata::RuntimeMetadata;
use scale_info_legacy::TypeRegistrySet;
use scale_type_resolver::TypeResolver;

pub type StorageValue = scale_value::Value<String>;
pub type StorageKeys = Vec<StorageKey>;

/// The decoded representation of a storage key.
pub struct StorageKey {
    pub hash: Vec<u8>,
    pub value: Option<scale_value::Value<String>>,
    pub hasher: frame_decode::storage::StorageHasher,
}

/// Decode the bytes representing some storage key. Here, we expect all of the key bytes including the hashed pallet name and storage entry.
pub fn decode_storage_keys(
    pallet_name: &str,
    storage_entry: &str,
    bytes: &[u8],
    metadata: &RuntimeMetadata,
    historic_types: &TypeRegistrySet,
    use_old_hashers: bool,
) -> anyhow::Result<StorageKeys> {
    match metadata {
        RuntimeMetadata::V8(m) => {
            decode_storage_keys_inner(pallet_name, storage_entry, bytes, m, historic_types, use_old_hashers)
        }
        RuntimeMetadata::V9(m) => {
            decode_storage_keys_inner(pallet_name, storage_entry, bytes, m, historic_types, use_old_hashers)
        }
        RuntimeMetadata::V10(m) => {
            decode_storage_keys_inner(pallet_name, storage_entry, bytes, m, historic_types, use_old_hashers)
        }
        RuntimeMetadata::V11(m) => {
            decode_storage_keys_inner(pallet_name, storage_entry, bytes, m, historic_types, use_old_hashers)
        }
        RuntimeMetadata::V12(m) => {
            decode_storage_keys_inner(pallet_name, storage_entry, bytes, m, historic_types, use_old_hashers)
        }
        RuntimeMetadata::V13(m) => {
            decode_storage_keys_inner(pallet_name, storage_entry, bytes, m, historic_types, use_old_hashers)
        }
        RuntimeMetadata::V14(m) => {
            decode_storage_keys_inner(pallet_name, storage_entry, bytes, m, &m.types, use_old_hashers)
        }
        RuntimeMetadata::V15(m) => {
            decode_storage_keys_inner(pallet_name, storage_entry, bytes, m, &m.types, use_old_hashers)
        }
        _ => bail!("Only metadata V8 - V15 is supported"),
    }
}

/// Decode the bytes representing some storage value.
pub fn decode_storage_value(
    pallet_name: &str,
    storage_entry: &str,
    bytes: &[u8],
    metadata: &RuntimeMetadata,
    historic_types: &TypeRegistrySet,
) -> anyhow::Result<StorageValue> {
    match metadata {
        RuntimeMetadata::V8(m) => {
            decode_storage_value_inner(pallet_name, storage_entry, bytes, m, historic_types)
        }
        RuntimeMetadata::V9(m) => {
            decode_storage_value_inner(pallet_name, storage_entry, bytes, m, historic_types)
        }
        RuntimeMetadata::V10(m) => {
            decode_storage_value_inner(pallet_name, storage_entry, bytes, m, historic_types)
        }
        RuntimeMetadata::V11(m) => {
            decode_storage_value_inner(pallet_name, storage_entry, bytes, m, historic_types)
        }
        RuntimeMetadata::V12(m) => {
            decode_storage_value_inner(pallet_name, storage_entry, bytes, m, historic_types)
        }
        RuntimeMetadata::V13(m) => {
            decode_storage_value_inner(pallet_name, storage_entry, bytes, m, historic_types)
        }
        RuntimeMetadata::V14(m) => {
            decode_storage_value_inner(pallet_name, storage_entry, bytes, m, &m.types)
        }
        RuntimeMetadata::V15(m) => {
            decode_storage_value_inner(pallet_name, storage_entry, bytes, m, &m.types)
        }
        _ => bail!("Only metadata V8 - V15 is supported"),
    }
}

fn decode_storage_keys_inner<Info, Resolver>(
    pallet_name: &str,
    storage_entry: &str,
    bytes: &[u8],
    info: &Info,
    type_resolver: &Resolver,
    use_old_hashers: bool,
) -> anyhow::Result<StorageKeys>
where
    Info: frame_decode::storage::StorageTypeInfo,
    Info::TypeId: Clone + core::fmt::Display + core::fmt::Debug + Send + Sync + 'static,
    Resolver: TypeResolver<TypeId = Info::TypeId>,
{
    let cursor = &mut &*bytes;

    let entry_info = info
        .storage_info(pallet_name, storage_entry)
        .map_err(|e| e.into_owned())?
        .use_use_old_v9_storage_hashers(use_old_hashers);

    let key_info = frame_decode::storage::decode_storage_key_with_info(
        cursor,
        &entry_info,
        type_resolver,
    )?;

    let decoded: anyhow::Result<_> = key_info
        .parts()
        .map(|part| {
            let hash = bytes[part.hash_range()].to_vec();
            let hasher = part.hasher();
            let value = part
                .value()
                .map(|val_info| {
                    let value = scale_value::scale::decode_as_type(
                        &mut &bytes[val_info.range()],
                        val_info.ty().clone(),
                        type_resolver,
                    )?
                    .map_context(|id| id.to_string());
                    anyhow::Result::<scale_value::Value<String>>::Ok(value)
                })
                .transpose()?;

            Ok(StorageKey {
                hash,
                value,
                hasher,
            })
        })
        .collect();

    if !cursor.is_empty() && decoded.is_ok() {
        let decoded = print_storage_key_res(&decoded)?;
        bail!(
            "{} leftover bytes decoding storage keys: {cursor:?}. decoded: {decoded}",
            cursor.len()
        );
    }

    decoded
}

fn print_storage_key_res(keys: &anyhow::Result<StorageKeys>) -> anyhow::Result<String> {
    match keys {
        Err(e) => Ok(format!("Error: {e}")),
        Ok(keys) => {
            let mut s = String::new();
            write_storage_keys_fmt(&mut s, keys)?;
            Ok(s)
        }
    }
}

pub fn write_storage_keys<W: std::io::Write>(writer: W, keys: &[StorageKey]) -> anyhow::Result<()> {
    let writer = crate::utils::ToFmtWrite(writer);
    write_storage_keys_fmt(writer, keys)
}

pub fn write_storage_keys_fmt<W: std::fmt::Write>(
    mut writer: W,
    keys: &[StorageKey],
) -> anyhow::Result<()> {
    // Plain entries have no keys:
    if keys.is_empty() {
        write!(&mut writer, "plain")?;
        return Ok(());
    }

    // blake2: AccountId(0x2331) + ident: Foo(123) + blake2:0x23edbfe
    for (idx, key) in keys.into_iter().enumerate() {
        if idx != 0 {
            write!(&mut writer, " + ")?;
        }

        match (key.hasher, &key.value) {
            (StorageHasher::Blake2_128, None) => {
                write!(&mut writer, "blake2_128: ")?;
                write!(&mut writer, "{}", hex::encode(&key.hash))?;
            }
            (StorageHasher::Blake2_256, None) => {
                write!(&mut writer, "blake2_256: ")?;
                write!(&mut writer, "{}", hex::encode(&key.hash))?;
            }
            (StorageHasher::Blake2_128Concat, Some(value)) => {
                write!(&mut writer, "blake2_128_concat: ")?;
                crate::utils::write_compact_value_fmt(&mut writer, &value)?;
            }
            (StorageHasher::Twox128, None) => {
                write!(&mut writer, "twox_128: ")?;
                write!(&mut writer, "{}", hex::encode(&key.hash))?;
            }
            (StorageHasher::Twox256, None) => {
                write!(&mut writer, "twox_256: ")?;
                write!(&mut writer, "{}", hex::encode(&key.hash))?;
            }
            (StorageHasher::Twox64Concat, Some(value)) => {
                write!(&mut writer, "twox64_concat: ")?;
                crate::utils::write_compact_value_fmt(&mut writer, &value)?;
            }
            (StorageHasher::Identity, Some(value)) => {
                write!(&mut writer, "ident: ")?;
                crate::utils::write_compact_value_fmt(&mut writer, &value)?;
            }
            _ => {
                bail!("Invalid storage hasher/value pair")
            }
        }
    }

    Ok(())
}

fn decode_storage_value_inner<Info, Resolver>(
    pallet_name: &str,
    storage_entry: &str,
    bytes: &[u8],
    info: &Info,
    type_resolver: &Resolver,
) -> anyhow::Result<StorageValue>
where
    Info: frame_decode::storage::StorageTypeInfo,
    Info::TypeId: Clone + core::fmt::Display + core::fmt::Debug + Send + Sync + 'static,
    Resolver: TypeResolver<TypeId = Info::TypeId>,
{
    let cursor = &mut &*bytes;
    let value = frame_decode::storage::decode_storage_value(
        pallet_name,
        storage_entry,
        cursor,
        info,
        type_resolver,
        scale_value::scale::ValueVisitor::new(),
    )?
    .map_context(|id| id.to_string());

    if !cursor.is_empty() {
        let mut value_string = String::new();
        crate::utils::write_value_fmt(&mut value_string, &value)?;
        bail!(
            "{} leftover bytes decoding storage value: {cursor:?}. decoded:\n\n{value_string}",
            cursor.len()
        );
    }

    Ok(value)
}
