"use client";

import { motion } from "framer-motion";
import { useEffect, useMemo, useState } from "react";

export function AnimatedText({ words }: { words: string[] }) {
  const [wordIndex, setWordIndex] = useState(0);
  const wordList = useMemo(() => words, [words]);

  useEffect(() => {
    const timeoutId = setTimeout(() => {
      if (wordIndex === wordList.length - 1) {
        setWordIndex(0);
      } else {
        setWordIndex(wordIndex + 1);
      }
    }, 2000);
    return () => clearTimeout(timeoutId);
  }, [wordIndex, wordList]);

  return (
    <div className="relative flex w-full justify-center overflow-visible text-center min-w-[10px] md:min-w-[240px] px-2">
      &nbsp;
      {wordList.map((word, index) => (
        <motion.span
          key={index}
          className="absolute font-extrabold italic text-3xl md:text-3xl bg-gradient-to-r from-blue-500 via-purple-500 to-pink-500 dark:from-blue-400 dark:via-purple-400 dark:to-pink-400 bg-clip-text text-transparent w-full inline-flex justify-center items-center"
          initial={{ opacity: 0, y: -60 }}
          animate={
            wordIndex === index
              ? {
                  y: 0,
                  opacity: 1,
                  transition: { opacity: { duration: 0.4 }, y: { type: 'spring', stiffness: 50 } }
                }
              : {
                  y: wordIndex > index ? -30 : 20,
                  opacity: 0,
                  transition: { opacity: { duration: 0.4 }, y: { type: 'spring', stiffness: 50 } }
                }
          }
        >
          {word}
        </motion.span>
      ))}
    </div>
  );
} 