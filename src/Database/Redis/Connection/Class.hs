{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE BangPatterns           #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts       #-}

module Database.Redis.Connection.Class
  ( PubSubConn(..)
  , ReqReplyConn(..)
  , request
  ) where

import Data.RESP (RespMessage, RespExpr)
import Data.ByteString (ByteString)

class ReqReplyConn conn where
  sendReqReplyMsg :: conn -> ByteString -> IO ()
  recvReqReplyMsg :: conn -> IO RespExpr

class PubSubConn conn where
  sendPubSubMsg :: conn -> ByteString -> IO ()
  recvPubSubMsg :: conn -> IO RespMessage

  -- | The **only** case where this connection will be flushed
  -- is when this is called.
  flushPubSubMsgs :: conn -> IO ()

-- |Send a request and receive the corresponding reply
request :: ReqReplyConn a => a -> ByteString -> IO RespExpr
request conn req = sendReqReplyMsg conn req >> recvReqReplyMsg conn
