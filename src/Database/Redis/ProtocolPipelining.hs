{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |A module for automatic, optimal protocol pipelining.
--
--  Protocol pipelining is a technique in which multiple requests are written
--  out to a single socket without waiting for the corresponding responses.
--  The pipelining of requests results in a dramatic improvement in protocol
--  performance.
--
--  [Optimal Pipelining] uses the least number of network packets possible
--
--  [Automatic Pipelining] means that requests are implicitly pipelined as much
--      as possible, i.e. as long as a request's response is not used before any
--      subsequent requests.
--
--  Note that this module is incompatible with push messages, as that would
--  listening for, and inspecting, messages requires flushing the pipeline.
--
module Database.Redis.ProtocolPipelining (
  Connection,
  connect,
  enableTLS,
  beginReceiving,
  disconnect,
  request,
  ppSend,
  ppRecv,
  fromCtx
) where

import           Prelude
import           Control.Monad
import           Data.RESP (parseMessage)
import qualified Scanner
import qualified Data.ByteString as S
import           Data.IORef
import qualified Network.Socket as NS
import qualified Network.TLS as TLS
import           System.IO.Unsafe

import           Database.Redis.Connection.Class (RedisConnection(..))
import qualified Database.Redis.ConnectionContext as CC
import           Database.Redis.Protocol

data Connection = Conn
  { connCtx        :: CC.ConnectionContext -- ^ Connection socket-handle.
  , connReplies    :: IORef [RespExpr] -- ^ Reply thunks for unsent requests.
  , connPending    :: IORef [RespExpr]
    -- ^ Reply thunks for requests "in the pipeline", meaning not yet forced.
    -- Refers to the same list as 'connReplies', but can have an offset.
  , connPendingCnt :: IORef Int
    -- ^ Number of pending replies and thus the difference length between
    --   'connReplies' and 'connPending'.
    --   length connPending  - pendingCount = length connReplies
  }

instance HasReqReplyConnection Connection where
  getReqReplyConn = id

instance RedisConnection Connection RespExpr where
  recvRedis = ppRecv
  sendRedis = ppSend

fromCtx :: CC.ConnectionContext -> IO Connection
fromCtx ctx = Conn ctx <$> newIORef [] <*> newIORef [] <*> newIORef 0

connect :: NS.HostName -> CC.PortID -> Maybe Int -> IO Connection
connect hostName portId timeoutOpt = do
    connCtx <- CC.connect hostName portId timeoutOpt
    connReplies <- newIORef []
    connPending <- newIORef []
    connPendingCnt <- newIORef 0
    return Conn{..}

enableTLS :: TLS.ClientParams -> Connection -> IO Connection
enableTLS tlsParams conn@Conn{..} = do
    newCtx <- CC.enableTLS tlsParams connCtx
    return conn{connCtx = newCtx}

beginReceiving :: Connection -> IO ()
beginReceiving conn = do
  rs <- connGetReplies conn
  writeIORef (connReplies conn) rs
  writeIORef (connPending conn) rs

disconnect :: Connection -> IO ()
disconnect Conn{..} = CC.disconnect connCtx

-- |Write the request to the socket output buffer, without actually sending.
--  The 'Handle' is 'hFlush'ed when reading replies from the 'connCtx'.
ppSend :: Connection -> S.ByteString -> IO ()
ppSend Conn{..} s = do
  CC.send connCtx s

  -- Signal that we expect one more reply from Redis.
  n <- atomicModifyIORef' connPendingCnt $ \n -> let n' = n+1 in (n', n')
  -- Limit the "pipeline length". This is necessary in long pipelines, to avoid
  -- thunk build-up, and thus space-leaks.
  -- TODO find smallest max pending with good-enough performance.
  when (n >= 1000) $ do
    -- Force oldest pending reply.
    r:_ <- readIORef connPending
    r `seq` return ()

-- |Take a reply-thunk from the list of future replies.
ppRecv :: Connection -> IO RespExpr
ppRecv Conn{..} = do
  r : rs <- readIORef connReplies
  writeIORef connReplies rs
  return r

-- |A list of all future 'Reply's of the 'Connection'.
--
--  The spine of the list can be evaluated without forcing the replies.
--
--  Evaluating/forcing a 'Reply' from the list will 'unsafeInterleaveIO' the
--  reading and parsing from the 'connCtx'. To ensure correct ordering, each
--  Reply first evaluates (and thus reads from the network) the previous one.
--
--  'unsafeInterleaveIO' only evaluates it's result once, making this function
--  thread-safe. 'Handle' as implemented by GHC is also threadsafe, it is safe
--  to call 'hFlush' here. The list constructor '(:)' must be called from
--  /within/ unsafeInterleaveIO, to keep the replies in correct order.
connGetReplies :: Connection -> IO [RespMessage]
connGetReplies conn@Conn{..} = go S.empty (RespReply $ RespString "previous of first")
  where
    go :: S.ByteString -> RespMessage -> IO [RespMessage]
    go rest previous = do
      -- lazy pattern match to actually delay the receiving
      ~(r :: RespMessage, rest' :: S.ByteString) <- unsafeInterleaveIO $ do
        -- Force previous reply for correct order.
        previous `seq` return ()
        scanResult <- Scanner.scanWith readMore parseMessage rest
        case scanResult of
          Scanner.Fail{}       -> CC.errConnClosed
          Scanner.More{}    -> error "Hedis: parseWith returned Partial"
          Scanner.Done rest' r -> do
            -- r is the same as 'head' of 'connPending'. Since we just
            -- received r, we remove it from the pending list.
            atomicModifyIORef' connPending $ \case
               (_:rs) -> (rs, ())
               [] -> error "Hedis: impossible happened parseWith missing value that it just received"
            -- We now expect one less reply from Redis. We don't count to
            -- negative, which would otherwise occur during pubsub.
            atomicModifyIORef' connPendingCnt $ \n -> (max 0 (n-1), ())
            return (r, rest')
      rs <- unsafeInterleaveIO (go rest' r)
      return (r:rs)

    readMore = CC.ioErrorToConnLost $ do
      flush conn
      CC.recv connCtx
