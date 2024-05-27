{-# LANGUAGE CPP #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Redis.Core.Internal where
#if __GLASGOW_HASKELL__ > 711 && __GLASGOW_HASKELL__ < 808
import Control.Monad.Fail (MonadFail)
#endif
import Control.Monad.Reader
import Control.Monad.IO.Unlift (MonadUnliftIO)

-- |Context for normal command execution, outside of transactions. Use
--  'runRedis' to run actions of this type.
--
--  In this context, each result is wrapped in an 'Either' to account for the
--  possibility of Redis returning an 'Error' reply.
newtype Redis conn a =
  Redis (ReaderT conn IO a)
  deriving (Monad, MonadIO, Functor, Applicative, MonadUnliftIO)

#if __GLASGOW_HASKELL__ > 711
deriving instance MonadFail (Redis conn)
#endif
