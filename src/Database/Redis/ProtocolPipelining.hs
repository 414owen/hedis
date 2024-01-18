{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}

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
module Database.Redis.ProtocolPipelining (
  Connection,
  connect, enableTLS, beginReceiving, disconnect, request, send, recv, flush, fromCtx
) where

import           Prelude
import qualified Scanner
import qualified Data.ByteString as S
import           Data.IORef
import qualified Network.Socket as NS
import qualified Network.TLS as TLS

import           Database.Redis.Protocol
import qualified Database.Redis.ConnectionContext as CC

data Connection = Conn
  { connCtx      :: CC.ConnectionContext -- ^ Connection socket-handle.
  , connGetReply :: IO Reply -- ^ Reply thunks for unsent requests.
  }


fromCtx :: CC.ConnectionContext -> IO Connection
fromCtx ctx = Conn ctx <$> mkConnGetReply ctx

connect :: NS.HostName -> CC.PortID -> Maybe Int -> IO Connection
connect hostName portId timeoutOpt = do
  connCtx <- CC.connect hostName portId timeoutOpt
  fromCtx connCtx

enableTLS :: TLS.ClientParams -> Connection -> IO Connection
enableTLS tlsParams conn@Conn{..} = do
    newCtx <- CC.enableTLS tlsParams connCtx
    return conn{connCtx = newCtx}

beginReceiving :: Connection -> IO ()
beginReceiving conn = return ()

disconnect :: Connection -> IO ()
disconnect Conn{..} = CC.disconnect connCtx

-- |Write the request to the socket output buffer, without actually sending.
--  The 'Handle' is 'hFlush'ed when reading replies from the 'connCtx'.
send :: Connection -> S.ByteString -> IO ()
send Conn{..} s = do
  CC.send connCtx s

-- |Take a reply-thunk from the list of future replies.
recv :: Connection -> IO Reply
recv conn@Conn{..} = do
  flush conn
  connGetReply

-- | Flush the socket.  Normally, the socket is flushed in 'recv' (actually 'conGetReplies'), but
-- for the multithreaded pub/sub code, the sending thread needs to explicitly flush the subscription
-- change requests.
flush :: Connection -> IO ()
flush Conn{..} = CC.flush connCtx

-- |Send a request and receive the corresponding reply
request :: Connection -> S.ByteString -> IO Reply
request conn req = send conn req >> recv conn

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
mkConnGetReply :: CC.ConnectionContext -> IO (IO Reply)
mkConnGetReply connCtx = do
  restRef <- newIORef S.empty
  pure $ go restRef
  where
    go restRef = do
      rest <- readIORef restRef
      scanResult <- Scanner.scanWith readMore reply rest
      case scanResult of
        Scanner.Fail{}    -> CC.errConnClosed
        Scanner.More{}    -> error "Hedis: parseWith returned Partial"
        Scanner.Done rest' r -> do
          writeIORef restRef rest'
          return r

    readMore = CC.ioErrorToConnLost $ do
      CC.recv connCtx
