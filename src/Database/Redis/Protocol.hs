{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Most of the protocol is now implemented in the `resp` library.
-}

module Database.Redis.Protocol
  ( RespMessage(..), RespExpr(..)
  , renderRequest, parseExpression
  ) where

import Prelude hiding (error, take)
#if __GLASGOW_HASKELL__ < 710
import Control.Applicative
#endif
import Control.DeepSeq
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.RESP

instance NFData RespMessage
instance NFData RespExpr

------------------------------------------------------------------------------
-- Request
--
renderRequest :: [ByteString] -> ByteString
renderRequest req = B.concat (argCnt:args)
  where
    argCnt = B.concat ["*", showBS (length req), crlf]
    args   = map renderArg req

renderArg :: ByteString -> ByteString
renderArg arg = B.concat ["$",  argLen arg, crlf, arg, crlf]
  where
    argLen = showBS . B.length

showBS :: (Show a) => a -> ByteString
showBS = B.pack . show

crlf :: ByteString
crlf = "\r\n"
