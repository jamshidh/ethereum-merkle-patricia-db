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
import qualified Data.Map as M
import Data.Monoid
import qualified Data.Set as S
import qualified Database.LevelDB as LD
import System.Exit
import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit

import qualified Data.NibbleString as N

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
    liftIO $ assertEqual "roundtrip in-out db didn't match" (M.fromList $ fmap rlpEncode <$> valuesIn) (M.fromList valuesOut)
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

testShortcutNodeDataInsert2::Assertion
testShortcutNodeDataInsert2 = do
  runResourceT $ do
    db <- openMPDB "/tmp/tmpDB"
    verifyDBDataIntegrity db
        [
          (N.EvenNibbleString $ BC.pack "abcd", BC.pack "abcd"),
          (N.EvenNibbleString $ BC.pack "bb", BC.pack "bb")
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

testOther::Assertion
testOther = do
  runResourceT $ do
    db <- openMPDB "/tmp/tmpDB"
    verifyDBDataIntegrity db [("", "abcd")]
    verifyDBDataIntegrity db [("0123", "dog"), ("0123", "cat")]
    verifyDBDataIntegrity db [("abcd", "abcd"), ("ab12", "bb"), ("ab21", "aefg")]
    verifyDBDataIntegrity db [("ab", "abcd"), ("bb", "bb"), ("cb", "aefg"), ("bc", "qq")]



main::IO ()
main = 
  defaultMainWithOpts 
  [
   testCase "ShortcutNodeData Insert" testShortcutNodeDataInsert,
   testCase "ShortcutNodeData2 Insert" testShortcutNodeDataInsert2,
   testCase "FullNodeData Insert" testFullNodeDataInsert,
   testCase "other" testOther
  ] mempty
