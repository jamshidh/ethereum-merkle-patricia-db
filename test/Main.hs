{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as BC
import Data.Default
import Data.Functor
import Data.List
import qualified Data.Map as M
import Data.Monoid
import qualified Database.LevelDB as LD
import System.Exit
import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit

import qualified Data.NibbleString as N

import Data.RLP
import Database.MerklePatricia

bigTest=
  [
    ("00000000000000000000000000000000ffffffffffffffff0000000000000000", "90467269656e647320262046616d696c79"),
    ("00000000000000000000000000000000ffffffffffffffff0000000000000001", "8772656631323334"),
    ("00000000000000000000000000000000ffffffffffffffff0000000000000002", "04"),
    ("00000000000000000000000000000000ffffffffffffffff0000000000000003", "84548123a8"),
    ("0000000000000000000000000000000000000000000000000000000000000000", "974c696162696c69746965733a496e697469616c4c6f616e"),
    ("0000000000000000000000000000000000000000000000000000000000000001", "a0fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe7960"),
    ("0000000000000000000000000000000000000000000000000000000000000002", "83555344"),
    ("0000000000000000000000000000000000000000000000010000000000000000", "8f4173736574733a436865636b696e67"),
    ("0000000000000000000000000000000000000000000000010000000000000001", "830186a0"),
    ("0000000000000000000000000000000000000000000000010000000000000002", "83555344"),
    ("00000000000000000000000000000002ffffffffffffffff0000000000000003", "84548123a8")
  ]




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

prefixIfNeeded::B.ByteString->B.ByteString
prefixIfNeeded s | odd $ B.length s = "0" `B.append` s
prefixIfNeeded s = s

testBigTest::Assertion
testBigTest = do
  runResourceT $ do
    db <- openMPDB "/tmp/tmpDB"
    verifyDBDataIntegrity db (fmap (fst . B16.decode . prefixIfNeeded) <$> bigTest)



main::IO ()
main = 
  defaultMainWithOpts 
  [
   testCase "ShortcutNodeData Insert" testShortcutNodeDataInsert,
   testCase "ShortcutNodeData2 Insert" testShortcutNodeDataInsert2,
   testCase "FullNodeData Insert" testFullNodeDataInsert,
   testCase "other" testOther,
   testCase "bigTest" testBigTest
  ] mempty
