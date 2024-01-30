{- | This module provides a connection that isn't pipelined, and can be
used to run request/reply commands, as well as publish/subscribe.
-}

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}

module Database.Redis.Connection.NotPipelined
  ( Connection
  , connect
  , disconnect
  , ppSend
  , ppRecv
  , fromCtx
  ) where

import           Data.RESP (parseMessage)
import qualified Scanner
import qualified Data.ByteString as S
import qualified Network.Socket as NS
import qualified Network.TLS as TLS

import           Database.Redis.Connection.Class (RedisConnection(..), HasReqReplyConnection (..))
import qualified Database.Redis.ConnectionContext as CC
import           Database.Redis.Protocol
import Control.Concurrent.MVar (newMVar, takeMVar, MVar, putMVar)

data Connection = Conn
  { connCtx :: {-# UNPACK #-} !CC.ConnectionContext -- ^ Connection socket-handle.
  , connLeftovers :: {-# UNPACK #-} !(MVar S.ByteString)
  }

instance HasReqReplyConnection Connection Connection RespExpr where
  getReqReplyConn = id

instance RedisConnection Connection RespExpr where
  recvRedisConn = ppRecv
  sendRedisConn = ppSend

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

ppSend :: Connection -> S.ByteString -> IO ()
ppSend Conn{ connCtx } s = do
  CC.send connCtx s
  CC.flush connCtx

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
    readMore = CC.ioErrorToConnLost $ do
      CC.flush connCtx
      CC.recv connCtx
