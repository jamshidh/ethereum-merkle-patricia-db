{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.Default
import Data.Functor
import Data.List
import Data.Monoid
import qualified Data.Set as S
import qualified Database.LevelDB as LD
import System.Exit
import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit

import qualified Data.NibbleString as N

{-
import Data.Address
import DB.EthDB
import Format
import DB.ModifyStateDB
import SHA
import Util
-}

import Data.RLP
import Database.MerklePatricia

putKeyVals::MPDB->[(N.NibbleString, B.ByteString)]->ResourceT IO MPDB
putKeyVals db [(k,v)] = putKeyVal db k (rlpEncode v)
putKeyVals db ((k, v):rest) = do
  db'<- putKeyVal db k $ rlpEncode v
  putKeyVals db' rest

verifyDBDataIntegrity::MPDB->[(N.NibbleString, B.ByteString)]->ResourceT IO ()
verifyDBDataIntegrity db valuesIn = do
    db2 <- putKeyVals db valuesIn
    --return (db, stateRoot2)
    valuesOut <- getKeyVals db2 (N.EvenNibbleString B.empty)
    liftIO $ assertEqual "empty db didn't match" (S.fromList $ fmap rlpEncode <$> valuesIn) (S.fromList valuesOut)
    return ()

testShortcutNodeDataInsert::Assertion
testShortcutNodeDataInsert = do
  runResourceT $ do
    db <- openMPDB "/tmp/tmpDB"
    verifyDBDataIntegrity db
        [
          (N.EvenNibbleString $ BC.pack "abcd", BC.pack "abcd"),
          (N.EvenNibbleString $ BC.pack "aefg", BC.pack "aefg")
        ]

testFullNodeDataInsert::Assertion
testFullNodeDataInsert = do
  runResourceT $ do
    db <- openMPDB "/tmp/tmpDB"
    verifyDBDataIntegrity db
        [
          (N.EvenNibbleString $ BC.pack "abcd", BC.pack "abcd"),
          (N.EvenNibbleString $ BC.pack "bb", BC.pack "bb"),
          (N.EvenNibbleString $ BC.pack "aefg", BC.pack "aefg")
        ]

main::IO ()
main = 
  defaultMainWithOpts 
  [
   testCase "ShortcutNodeData Insert" testShortcutNodeDataInsert,
   testCase "FullNodeData Insert" testFullNodeDataInsert
  ] mempty
