{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE CPP, FlexibleInstances, TypeSynonymInstances,
    OverloadedStrings #-}

#if __GLASGOW_HASKELL__ < 710
{-# LANGUAGE OverlappingInstances #-}
#endif

module Database.Redis.Types where

#if __GLASGOW_HASKELL__ < 710
import Control.Applicative
#endif
import Data.Int
import Control.DeepSeq
import Data.ByteString.Char8 (ByteString, pack)
import qualified Data.ByteString.Lex.Fractional as F (readSigned, readExponential)
import qualified Data.ByteString.Lex.Integral as I (readSigned, readDecimal)
import GHC.Generics

import Database.Redis.Protocol


------------------------------------------------------------------------------
-- Classes of types Redis understands
--
class RedisArg a where
    encode :: a -> ByteString

class RedisResult a where
    decode :: RespExpr -> Either RespExpr a

------------------------------------------------------------------------------
-- RedisArg instances
--
instance RedisArg ByteString where
    encode = id

instance RedisArg Integer where
    encode = pack . show

instance RedisArg Int64 where
    encode = pack . show

instance RedisArg Double where
    encode a
        | isInfinite a && a > 0 = "+inf"
        | isInfinite a && a < 0 = "-inf"
        | otherwise = pack . show $ a

------------------------------------------------------------------------------
-- RedisResult instances
--
data Status = Ok | Pong | Status ByteString
    deriving (Show, Eq, Generic)

instance NFData Status

data RedisType = None | String | Hash | List | Set | ZSet
    deriving (Show, Eq)

instance RedisResult RespExpr where
    decode = Right

instance RedisResult ByteString where
    decode (RespString s) = Right s
    decode (RespBlob s)   = Right s
    decode r              = Left r

instance RedisResult Integer where
    decode (RespInteger n) = Right $ fromIntegral n
    decode r           =
        maybe (Left r) (Right . fst) . I.readSigned I.readDecimal =<< decode r

instance RedisResult Int64 where
    decode (RespInteger n) = Right n
    decode r           =
        maybe (Left r) (Right . fst) . I.readSigned I.readDecimal =<< decode r

instance RedisResult Double where
    decode (RespDouble d) = Right d
    decode r = maybe (Left r) (Right . fst) . F.readSigned F.readExponential =<< decode r

instance RedisResult Status where
    decode (RespString s) = Right $ case s of
        "OK"     -> Ok
        "PONG"   -> Pong
        _        -> Status s
    decode r = Left r

instance RedisResult RedisType where
    decode (RespString s) = Right $ case s of
        "none"   -> None
        "string" -> String
        "hash"   -> Hash
        "list"   -> List
        "set"    -> Set
        "zset"   -> ZSet
        _        -> error $ "Hedis: unhandled redis type: " ++ show s
    decode r = Left r

instance RedisResult Bool where
    decode (RespBool True)  = Right True
    decode (RespBool False) = Right True
    decode (RespInteger 1)  = Right True
    decode (RespInteger 0)  = Right False
    decode RespNull         = Right False -- Lua boolean false = nil bulk reply
    decode r                = Left r

instance (RedisResult a) => RedisResult (Maybe a) where
    decode RespNull = Right Nothing
    decode r        = Just <$> decode r

instance
#if __GLASGOW_HASKELL__ >= 710
    {-# OVERLAPPABLE #-}
#endif
    (RedisResult a) => RedisResult [a] where
    decode (RespArray rs) = mapM decode rs
    decode r              = Left r
 
instance (RedisResult a, RedisResult b) => RedisResult (a,b) where
    decode (RespArray [x, y]) = (,) <$> decode x <*> decode y
    decode r                  = Left r

instance (RedisResult k, RedisResult v) => RedisResult [(k,v)] where
    decode r = case r of
                (RespArray rs) -> pairs rs
                _              -> Left r
      where
        pairs []         = Right []
        pairs (_:[])     = Left r
        pairs (r1:r2:rs) = do
            k   <- decode r1
            v   <- decode r2
            kvs <- pairs rs
            return $ (k,v) : kvs
