{-# LANGUAGE AllowAmbiguousTypes, OverloadedStrings, GeneralizedNewtypeDeriving, RecordWildCards,
    MultiParamTypeClasses, FunctionalDependencies, FlexibleInstances, CPP,
    DeriveDataTypeable, StandaloneDeriving, UndecidableInstances #-}

module Database.Redis.Core (
    Redis(), unRedis, reRedis,
    RedisCtx(..), MonadRedis(..),
    send, recvReply, sendRequest,
    runRedisInternal,
    runRedisClusteredInternal,
    RedisEnv(..),
) where

import Prelude
#if __GLASGOW_HASKELL__ < 710
import Control.Applicative
#endif
import Control.Monad.Reader
import qualified Data.ByteString as B
import Data.IORef
import Database.Redis.Core.Internal
import Database.Redis.Protocol
import qualified Database.Redis.Connection.ProtocolPipelining as PP
import Database.Redis.Types
import Database.Redis.Connection.Cluster(ShardMap)
import qualified Database.Redis.Connection.Cluster as Cluster
import Database.Redis.Connection.Class

--------------------------------------------------------------------------------
-- The Redis Monad
--

-- |This class captures the following behaviour: In a context @m@, a command
--  will return its result wrapped in a \"container\" of type @f@.
--
--  Please refer to the Command Type Signatures section of this page for more
--  information.
class (MonadRedis conn m) => RedisCtx m conn f | m -> f where
    returnDecode :: RedisResult a => RespExpr -> m (f a)

class (Monad m) => MonadRedis conn m where
    liftRedis :: Redis conn a -> m a

instance {-# OVERLAPPABLE #-}
  ( MonadTrans t
  , MonadRedis conn m
  , Monad (t m)
  ) => MonadRedis conn (t m) where
  liftRedis = lift . liftRedis

instance RedisCtx (Redis conn) conn (Either RespExpr) where
    returnDecode = return . decode

instance MonadRedis conn (Redis conn) where
    liftRedis = id

-- |Deconstruct Redis constructor.
--
--  'unRedis' and 'reRedis' can be used to define instances for
--  arbitrary typeclasses.
--
--  WARNING! These functions are considered internal and no guarantee
--  is given at this point that they will not break in future.
unRedis :: Redis conn a -> ReaderT conn IO a
unRedis (Redis r) = r

-- |Reconstruct Redis constructor.
reRedis :: ReaderT conn IO a -> Redis conn a
reRedis = Redis

-- |Internal version of 'runRedis' that does not depend on the 'Connection'
--  abstraction. Used to run the AUTH command when connecting.
runRedisInternal :: PP.Connection -> Redis conn a -> IO a
runRedisInternal conn (Redis redis) = do
  -- Dummy reply in case no request is sent.
  ref <- newIORef $ RespString "nobody will ever see this"
  r <- runReaderT redis (NonClusteredEnv conn ref)
  -- Evaluate last reply to keep lazy IO inside runRedis.
  readIORef ref >>= (`seq` return ())
  return r

runRedisClusteredInternal :: Cluster.Connection -> IO ShardMap -> Redis conn a -> IO a
runRedisClusteredInternal connection refreshShardmapAction (Redis redis) = do
    r <- runReaderT redis (ClusteredEnv refreshShardmapAction connection)
    r `seq` return ()
    return r

setLastReply :: RespExpr -> ReaderT PP.Connection IO ()
setLastReply r = do
  ref <- asks envLastReply
  lift (writeIORef ref r)

recvReply :: (MonadRedis conn m) => m RespExpr
recvReply = liftRedis $ Redis $ do
  conn <- asks envConn
  r <- liftIO (recvReqReplyMsg conn)
  return r

send :: (MonadRedis conn m) => [B.ByteString] -> m ()
send req = liftRedis $ Redis $ do
    conn <- asks envConn
    liftIO $ sendReqReplyMsg conn (renderRequest req)

-- |'sendRequest' can be used to implement commands from experimental
--  versions of Redis. An example of how to implement a command is given
--  below.
--
-- @
-- -- |Redis DEBUG OBJECT command
-- debugObject :: ByteString -> 'Redis' (Either 'Reply' ByteString)
-- debugObject key = 'sendRequest' [\"DEBUG\", \"OBJECT\", key]
-- @
--
sendRequest :: (ReqReplyConn conn, RedisCtx m conn f, RedisResult a)
    => [B.ByteString] -> m (f a)
sendRequest req = liftRedis $ Redis $ liftIO $ request envConn (renderRequest req) >>= returnDecode
