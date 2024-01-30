{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE BangPatterns           #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts       #-}

module Database.Redis.Connection.Class
  ( RedisConnection(..)
  , HasPubSubConnection(..)
  , HasReqReplyConnection(..)
  , HasScanner(..)
  , recvReply
  , request
  , send
  ) where

import Data.RESP (RespMessage, parseMessage, RespExpr, parseExpression)
import Data.ByteString (ByteString)
import Scanner (Scanner)

class ReqReplyConn conn where
  request :: conn -> IO RespExpr

class PubSubConn conn where
  sendPubSubMsg :: conn -> [ByteString] -> IO ()
  recvRedisConn :: conn -> IO RespMessage

recvReply :: HasReqReplyConnection a conn RespExpr => a -> IO RespExpr
recvReply conn' = recvRedisConn conn
  where
    !conn = getReqReplyConn conn'

-- | Receive something on the push channel
recvPush :: HasReqReplyConnection a conn RespExpr => a -> IO RespMessage
recvPush conn' = recvRedisConn conn
  where
    !conn = getReqReplyConn conn'

-- |Send a request and receive the corresponding reply
request :: HasReqReplyConnection a b RespExpr => a -> ByteString -> IO RespExpr
request conn' req = sendRedisConn conn req >> recvRedisConn conn
  where
    !conn = getReqReplyConn conn'

-- |Send a request and receive the corresponding reply
send :: HasReqReplyConnection a b RespExpr => a -> ByteString -> IO ()
send conn' = sendRedisConn conn
  where
    !conn = getReqReplyConn conn'

class HasScanner a where
  getScanner :: Scanner a

instance HasScanner RespMessage where
  getScanner = parseMessage

instance HasScanner RespExpr where
  getScanner = parseExpression
