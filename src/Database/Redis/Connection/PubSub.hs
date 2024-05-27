{-# LANGUAGE RecordWildCards #-}

{-|

This module exposes a connection type that wraps two connections, one for pub/
sub, and one for request/reply. Why is this? Well, because prior to RESP3
(supported by redis ~6.0, experimental), it wasn't possible to do pub/sub and
request/reply on the same connection at all.

With the advent of RESP3, it's possible to do both on the same connection, but
not with automatic pipelining via laziness, because the pub/sub handler needs to
constantly listen for, and inspect, messages.

The solution? Open two connections, one for pub/sub, and one for request/reply.
Two connections won't break the bank.

-}

module Database.Redis.Connection.PubSub
  ( Connection(..)
  , connect
  , disconnect
  ) where

import Database.Redis.Connection.Class

import qualified Database.Redis.ConnectionContext             as CC
import qualified Database.Redis.Connection.ProtocolPipelining as ReqRep
import qualified Database.Redis.Connection.NotPipelined       as PubSub
import qualified Network.Socket                               as NS
import qualified Network.TLS                                  as TLS

data Connection
  = Connection
  { reqReplyConn :: {-# UNPACK #-} !ReqRep.Connection
  , pubSubConn   :: {-# UNPACK #-} !PubSub.Connection
  }

instance ReqReplyConn Connection where
  recvReqReplyMsg = recvReqReplyMsg . reqReplyConn
  sendReqReplyMsg = sendReqReplyMsg . reqReplyConn

instance PubSubConn Connection where
  recvPubSubMsg = recvPubSubMsg . pubSubConn
  sendPubSubMsg = sendPubSubMsg . pubSubConn

connect :: NS.HostName -> CC.PortID -> Maybe Int -> Maybe TLS.ClientParams -> IO Connection
connect hostName portId timeoutOpt mTlsOpts
  = Connection
  <$> ReqRep.connect hostName portId timeoutOpt mTlsOpts
  <*> PubSub.connect hostName portId timeoutOpt mTlsOpts

disconnect :: Connection -> IO ()
disconnect Connection{..} = PubSub.disconnect pubSubConn >> ReqRep.disconnect reqReplyConn
