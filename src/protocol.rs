//! Wire protocol `/filemesh/2.0.0`.
//!
//! Everything is a request/response pair handled by libp2p's `request_response`
//! behaviour, so each exchange gets its own stream, many can run concurrently,
//! and timeouts are handled for us.
//!
//! Frames are `u32 big-endian length | postcard payload`. The length is checked
//! against [`MAX_FRAME`] and the payload is read through a `take`, so a hostile
//! peer cannot make us pre-allocate more than we have actually received.

use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response, StreamProtocol};
use serde::{Deserialize, Serialize};
use std::io;

use crate::manifest::{FileId, Manifest};

pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/filemesh/2.0.0");

/// Hard cap on any single frame (also bounds the manifest size).
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    /// "I have a file for you." Carries no data; the receiver decides.
    Offer {
        id: FileId,
        name: String,
        size: u64,
    },
    GetManifest {
        id: FileId,
    },
    GetChunk {
        id: FileId,
        index: u32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    OfferReceived,
    Manifest(Manifest),
    Chunk {
        index: u32,
        /// Encoded as a single byte string (bulk copy), not element by element.
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    Error(ErrorCode),
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    NotFound,
    OutOfRange,
    Refused,
    Internal,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ErrorCode::NotFound => "file not found on peer",
            ErrorCode::OutOfRange => "chunk index out of range",
            ErrorCode::Refused => "peer refused the request",
            ErrorCode::Internal => "peer hit an internal error",
        };
        f.write_str(s)
    }
}

pub type Behaviour = request_response::Behaviour<FileMeshCodec>;
pub type TransferEvent = request_response::Event<Request, Response>;

pub fn new_behaviour() -> Behaviour {
    request_response::Behaviour::with_codec(
        FileMeshCodec,
        [(PROTOCOL, request_response::ProtocolSupport::Full)],
        request_response::Config::default()
            // A 1 MiB chunk on a slow link needs more than the 10 s default.
            .with_request_timeout(std::time::Duration::from_secs(60)),
    )
}

#[derive(Clone, Default)]
pub struct FileMeshCodec;

#[async_trait]
impl request_response::Codec for FileMeshCodec {
    type Protocol = StreamProtocol;
    type Request = Request;
    type Response = Response;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_frame(io).await?;
        decode(&bytes)
    }

    async fn read_response<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_frame(io).await?;
        decode(&bytes)
    }

    async fn write_request<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        req: Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_frame(io, &encode(&req)?).await
    }

    async fn write_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        res: Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_frame(io, &encode(&res)?).await
    }
}

fn encode<M: Serialize>(msg: &M) -> io::Result<Vec<u8>> {
    postcard::to_allocvec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn decode<M: for<'de> Deserialize<'de>>(bytes: &[u8]) -> io::Result<M> {
    postcard::from_bytes(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

async fn read_frame<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {} bytes exceeds limit of {}", len, MAX_FRAME),
        ));
    }

    // `take` + `read_to_end` grows the buffer only as bytes really arrive.
    let mut buf = Vec::new();
    (&mut *io).take(len as u64).read_to_end(&mut buf).await?;
    if buf.len() != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "stream ended before the full frame arrived",
        ));
    }
    Ok(buf)
}

async fn write_frame<T>(io: &mut T, payload: &[u8]) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    if payload.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "outgoing frame exceeds MAX_FRAME",
        ));
    }
    io.write_all(&(payload.len() as u32).to_be_bytes()).await?;
    io.write_all(payload).await?;
    io.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;

    #[test]
    fn request_roundtrip() {
        futures::executor::block_on(async {
            let req = Request::GetChunk {
                id: [7u8; 32],
                index: 42,
            };
            let mut wire = Vec::new();
            write_frame(&mut wire, &encode(&req).unwrap()).await.unwrap();

            let mut cursor = Cursor::new(wire);
            let bytes = read_frame(&mut cursor).await.unwrap();
            match decode::<Request>(&bytes).unwrap() {
                Request::GetChunk { id, index } => {
                    assert_eq!(id, [7u8; 32]);
                    assert_eq!(index, 42);
                }
                other => panic!("unexpected {:?}", other),
            }
        });
    }

    #[test]
    fn response_with_chunk_roundtrip() {
        futures::executor::block_on(async {
            let data = vec![0xABu8; 1024 * 1024];
            let res = Response::Chunk {
                index: 3,
                data: data.clone(),
            };
            let mut wire = Vec::new();
            write_frame(&mut wire, &encode(&res).unwrap()).await.unwrap();
            let mut cursor = Cursor::new(wire);
            let bytes = read_frame(&mut cursor).await.unwrap();
            match decode::<Response>(&bytes).unwrap() {
                Response::Chunk { index, data: d } => {
                    assert_eq!(index, 3);
                    assert_eq!(d, data);
                }
                other => panic!("unexpected {:?}", other),
            }
        });
    }

    #[test]
    fn rejects_oversized_length_without_allocating() {
        futures::executor::block_on(async {
            // Claims a ~4 GiB frame but sends nothing.
            let mut cursor = Cursor::new(u32::MAX.to_be_bytes().to_vec());
            let err = read_frame(&mut cursor).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        });
    }

    #[test]
    fn truncated_frame_is_an_error() {
        futures::executor::block_on(async {
            let mut wire = 100u32.to_be_bytes().to_vec();
            wire.extend_from_slice(&[1, 2, 3]); // far fewer than 100 bytes
            let mut cursor = Cursor::new(wire);
            let err = read_frame(&mut cursor).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        });
    }

    #[test]
    fn garbage_payload_is_invalid_data() {
        let err = decode::<Request>(&[0xff, 0xff, 0xff]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
