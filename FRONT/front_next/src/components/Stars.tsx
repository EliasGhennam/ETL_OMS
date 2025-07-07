"use client";

import { useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import { Stars as DreiStars } from '@react-three/drei';
import { Points } from 'three';

export function Stars() {
  const starsRef = useRef<Points>(null);

  useFrame(({ clock }) => {
    if (starsRef.current) {
      starsRef.current.rotation.y = clock.getElapsedTime() * 0.05;
    }
  });

  return (
    <DreiStars
      ref={starsRef}
      radius={100}
      depth={50}
      count={5000}
      factor={4}
      saturation={0}
      fade
      speed={1}
    />
  );
} 