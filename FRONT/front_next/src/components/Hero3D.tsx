"use client";

import { Canvas, useFrame } from "@react-three/fiber";
import { OrbitControls, useGLTF } from "@react-three/drei";
import { Suspense, useRef, useEffect } from "react";
import { Group } from "three";

function Globe() {
  const globeRef = useRef<Group>(null);
  const { scene } = useGLTF('/models/Globe3D.glb');

  useEffect(() => {
    console.log('Model loaded:', scene);
    scene.traverse((child: { isMesh?: boolean; material?: { metalness?: number; roughness?: number; envMapIntensity?: number } }) => {
      if (child.isMesh) {
        child.material.metalness = 0.5;
        child.material.roughness = 1;
        child.material.envMapIntensity = 1;
      }
    });
  }, [scene]);

  useFrame(({ clock }) => {
    if (globeRef.current) {
      globeRef.current.rotation.y = clock.getElapsedTime() * 0.01;
    }
  });

  return (
    <group>
      <primitive 
        ref={globeRef}
        object={scene}
        scale={2}
        position={[0, 0, 0]}
      />
    </group>
  );
}

export function Hero3D() {
  return (
    <div className="h-full w-full flex items-end justify-center relative">
      <div className="w-screen h-screen max-w-full max-h-full">
        <Canvas
          camera={{ position: [4, 4, 4], fov: 60 }}
          style={{ background: "transparent" }}
        >
          <ambientLight intensity={1.5} />
          <directionalLight position={[5, 5, 5]} intensity={2} />
          <directionalLight position={[-5, -5, -5]} intensity={2} />
          <Suspense fallback={null}>
            <Globe />
            <OrbitControls 
              enableZoom={false}
              enablePan={false}
              autoRotate
              autoRotateSpeed={0.1}
            />
          </Suspense>
        </Canvas>
      </div>
    </div>
  );
} 