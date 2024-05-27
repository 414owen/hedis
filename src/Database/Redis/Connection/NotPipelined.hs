{- | This module provides a connection that isn't pipelined. It's currently
used as a component of 'Database.Redis.Connection.PubSub.Connection'.

It can also be used as a connection in its own right, which *only* supports
pub/sub.

In future, if someone separates out responses by type, this could be used as a
connection which supports both pub/sub and request/reply, over a single OS
socket. I recommend checking that doing so doesn't regress performance, before
merging such a change.
-}

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns        #-}

module Database.Redis.Connection.NotPipelined
  ( Connection
  , connect
  , disconnect
  ) where

import           Data.RESP (parseMessage)
import qualified Scanner
import qualified Data.ByteString as S
import qualified Network.Socket  as NS
import qualified Network.TLS     as TLS

import           Control.Concurrent.MVar (newMVar, takeMVar, MVar, putMVar)
import           Database.Redis.Connection.Class (PubSubConn(..))
import qualified Database.Redis.ConnectionContext as CC
import           Database.Redis.Protocol

data Connection = Conn
  { connCtx       :: {-# UNPACK #-} !CC.ConnectionContext -- ^ Connection socket-handle.
  , connLeftovers :: {-# UNPACK #-} !(MVar S.ByteString)
  }

instance PubSubConn Connection where
  recvPubSubMsg = ppRecv
  sendPubSubMsg Conn{ connCtx } s = CC.send connCtx s
  flushPubSubMsgs Conn{ connCtx} = CC.flush connCtx

fromCtx :: CC.ConnectionContext -> IO Connection
fromCtx connCtx = do
  connLeftovers <- newMVar mempty
  pure $ Conn { connCtx, connLeftovers }

connect :: NS.HostName -> CC.PortID -> Maybe Int -> Maybe TLS.ClientParams -> IO Connection
connect hostName portId timeoutOpt mTlsParams = do
  connCtx <- CC.connect hostName portId timeoutOpt
  case mTlsParams of
    Just tlsParams -> fromCtx =<< CC.enableTLS tlsParams connCtx
    Nothing -> fromCtx connCtx

disconnect :: Connection -> IO ()
disconnect Conn{ connCtx = connCtx } = CC.disconnect connCtx

ppRecv :: Connection -> IO RespMessage
ppRecv Conn{ connCtx, connLeftovers } = do
  leftovers <- takeMVar connLeftovers
  scanResult <- Scanner.scanWith readMore parseMessage leftovers
  case scanResult of
    Scanner.Fail{} -> CC.errConnClosed
    Scanner.More{} -> error "Hedis: parseWith returned Partial"
    Scanner.Done rest r -> do
      putMVar connLeftovers rest
      pure r

  where
    readMore = CC.ioErrorToConnLost $ CC.recv connCtx
