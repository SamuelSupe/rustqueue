use axum::http::Uri;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForwardMetadata {
    pub path_and_query: String,
    pub declared_bytes: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForwardMetadataError {
    Invalid,
    BodyTooLarge,
}

pub fn parse_forward_metadata(
    target: &str,
    content_length: Option<&str>,
    max_body_bytes: usize,
) -> Result<ForwardMetadata, ForwardMetadataError> {
    let uri: Uri = target.parse().map_err(|_| ForwardMetadataError::Invalid)?;
    if uri.scheme().is_some() || uri.authority().is_some() {
        return Err(ForwardMetadataError::Invalid);
    }
    let declared_bytes = content_length
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| ForwardMetadataError::Invalid)
        })
        .transpose()?;
    if declared_bytes.is_some_and(|bytes| bytes > max_body_bytes) {
        return Err(ForwardMetadataError::BodyTooLarge);
    }
    Ok(ForwardMetadata {
        path_and_query: uri
            .path_and_query()
            .map_or_else(|| "/".into(), ToString::to_string),
        declared_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_origin_form_and_rejects_absolute_form() {
        assert_eq!(
            parse_forward_metadata("/pub?topic=x", Some("3"), 4).unwrap(),
            ForwardMetadata {
                path_and_query: "/pub?topic=x".into(),
                declared_bytes: Some(3),
            }
        );
        assert_eq!(
            parse_forward_metadata("http://elsewhere/pub", None, 4),
            Err(ForwardMetadataError::Invalid)
        );
        assert_eq!(
            parse_forward_metadata("/pub", Some("5"), 4),
            Err(ForwardMetadataError::BodyTooLarge)
        );
    }
}
