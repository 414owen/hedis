{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE BangPatterns           #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts       #-}

module Database.Redis.Connection.Class
  ( RedisConnection(..)
  , HasPubSubConnection(..)
  , HasReqReplyConnection(..)
  , HasScanner(..)
  ) where

import Data.RESP (RespMessage, parseMessage, RespExpr, parseExpression)
import Data.ByteString (ByteString)
import Scanner (Scanner)

class RedisConnection conn msg => HasPubSubConnection a conn msg | a -> conn where
  getPubSubConn :: a -> conn

class RedisConnection conn msg => HasReqReplyConnection a conn msg | a -> conn where
  getReqReplyConn :: a -> conn

class RedisConnection conn msg | conn -> msg where
  recvRedisConn :: conn -> IO message
  sendRedisConn :: conn -> ByteString -> IO ()

recvReply :: HasReqReplyConnection a conn RespExpr => a -> IO RespExpr
recvReply conn' = recvRedisConn conn
  where
    !conn = getReqReplyConn conn'

-- |Send a request and receive the corresponding reply
request :: HasReqReplyConnection a b RespExpr => a -> ByteString -> IO RespExpr
request conn' req = sendRedisConn conn req >> recvRedisConn conn
  where
    !conn = getReqReplyConn conn'

class HasScanner a where
  getScanner :: Scanner a

instance HasScanner RespMessage where
  getScanner = parseMessage

instance HasScanner RespExpr where
  getScanner = parseExpression
