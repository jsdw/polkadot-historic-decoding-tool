use anyhow::bail;
use frame_metadata::RuntimeMetadata;
use scale_info_legacy::TypeRegistrySet;
use scale_type_resolver::TypeResolver;
use subxt::utils::{AccountId32, to_hex};

#[derive(Debug)]
pub enum Extrinsic {
    Unsigned {
        call_data: ExtrinsicCallData,
    },
    Signed {
        address: String,
        signature: String,
        signed_exts: Vec<(String, scale_value::Value<String>)>,
        call_data: ExtrinsicCallData,
    },
    General {
        signed_exts: Vec<(String, scale_value::Value<String>)>,
        call_data: ExtrinsicCallData,
    },
}

#[derive(Debug)]
pub struct ExtrinsicCallData {
    pub pallet_name: String,
    pub call_name: String,
    pub args: Vec<(String, scale_value::Value<String>)>,
}

pub fn decode_extrinsic(
    bytes: &[u8],
    metadata: &RuntimeMetadata,
    historic_types: &TypeRegistrySet,
) -> anyhow::Result<Extrinsic> {
    let ext = match metadata {
        RuntimeMetadata::V8(m) => decode_extrinsic_inner(bytes, m, historic_types),
        RuntimeMetadata::V9(m) => decode_extrinsic_inner(bytes, m, historic_types),
        RuntimeMetadata::V10(m) => decode_extrinsic_inner(bytes, m, historic_types),
        RuntimeMetadata::V11(m) => decode_extrinsic_inner(bytes, m, historic_types),
        RuntimeMetadata::V12(m) => decode_extrinsic_inner(bytes, m, historic_types),
        RuntimeMetadata::V13(m) => decode_extrinsic_inner(bytes, m, historic_types),
        RuntimeMetadata::V14(m) => decode_extrinsic_inner(bytes, m, &m.types),
        RuntimeMetadata::V15(m) => decode_extrinsic_inner(bytes, m, &m.types),
        _ => bail!("Only metadata V8 - V15 is supported"),
    }?;

    Ok(ext)
}

fn decode_extrinsic_inner<Info, Resolver>(
    bytes: &[u8],
    args_info: &Info,
    type_resolver: &Resolver,
) -> anyhow::Result<Extrinsic>
where
    Info: frame_decode::extrinsics::ExtrinsicTypeInfo,
    Info::TypeId: Clone + core::fmt::Display + core::fmt::Debug + Send + Sync + 'static,
    Resolver: TypeResolver<TypeId = Info::TypeId>,
{
    let cursor = &mut &*bytes;
    let extrinsic_info =
        frame_decode::extrinsics::decode_extrinsic(cursor, args_info, type_resolver)?;

    // Decode each call data argument into a Value<String>
    let call_data = {
        let args = extrinsic_info
            .call_data()
            .map(|arg| {
                let decoded_arg = scale_value::scale::decode_as_type(
                    &mut &bytes[arg.range()],
                    arg.ty().clone(),
                    type_resolver,
                )?
                .map_context(|ctx| ctx.to_string());
                Ok((arg.name().to_owned(), decoded_arg))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        ExtrinsicCallData {
            pallet_name: extrinsic_info.pallet_name().to_owned(),
            call_name: extrinsic_info.call_name().to_owned(),
            args,
        }
    };

    // If present, extract/decode the signature details.
    let signature = if let Some(signature_info) = extrinsic_info.signature_payload() {
        let address_bytes = &bytes[signature_info.address_range()];
        let address_string = address_bytes
            .try_into()
            .map(|b| AccountId32(b).to_string())
            .unwrap_or_else(|_e| format!("0x{}", hex::encode(address_bytes)));

        let signature_bytes = &bytes[signature_info.signature_range()];
        let signature_string = to_hex(signature_bytes);

        Some((address_string, signature_string))
    } else {
        None
    };

    let extensions = if let Some(exts) = extrinsic_info.transaction_extension_payload() {
        let signed_exts = exts
            .iter()
            .map(|signed_ext| {
                let decoded_ext = scale_value::scale::decode_as_type(
                    &mut &bytes[signed_ext.range()],
                    signed_ext.ty().clone(),
                    type_resolver,
                )?
                .map_context(|ctx| ctx.to_string());
                Ok((signed_ext.name().to_owned(), decoded_ext))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Some(signed_exts)
    } else {
        None
    };

    // If we didn't consume all of the bytes decoding the ext, complain.
    if !cursor.is_empty() {
        use std::fmt::Write;
        let mut s = String::new();

        writeln!(
            s,
            "{} leftover bytes found when trying to decode {}.{} with args:",
            cursor.len(),
            extrinsic_info.pallet_name(),
            extrinsic_info.call_name()
        )?;
        for (arg_name, arg_value) in call_data.args {
            write!(s, "  {arg_name}: ")?;
            crate::utils::write_value_fmt(&mut s, &arg_value)?;
            writeln!(s)?;
        }

        writeln!(s, "leftover bytes: 0x{}", hex::encode(cursor))?;
        bail!("{s}");
    }

    match (signature, extensions) {
        (Some((address, signature)), Some(signed_exts)) => Ok(Extrinsic::Signed {
            address,
            signature,
            signed_exts,
            call_data,
        }),
        (None, Some(signed_exts)) => Ok(Extrinsic::General {
            signed_exts,
            call_data,
        }),
        _ => Ok(Extrinsic::Unsigned { call_data }),
    }
}
