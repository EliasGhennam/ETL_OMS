"use client";

import { Canvas, useFrame } from "@react-three/fiber";
import { OrbitControls, useGLTF, Stars } from "@react-three/drei";
import { Suspense, useRef, useEffect } from "react";
import { Group } from "three";

function Globe() {
  const globeRef = useRef<Group>(null);
  const { scene } = useGLTF('/models/Globe3D.glb');

  useEffect(() => {
    console.log('Model loaded:', scene);
    scene.traverse((child: any) => {
      if (child.isMesh) {
        child.material.metalness = 0.5;
        child.material.roughness = 0;
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
        scale={15}
        position={[0, 0, 0]}
      />
    </group>
  );
}

export function Hero3D() {
  return (
    <div className="h-full w-full">
      <Canvas
        camera={{ position: [0, 0, 5], fov: 75 }}
        style={{ background: "transparent" }}
      >
        <ambientLight intensity={1.5} />
        <directionalLight position={[5, 5, 5]} intensity={2} />
        <directionalLight position={[-5, -5, -5]} intensity={2} />
        <Suspense fallback={null}>
          <Stars radius={100} depth={100} count={5000} factor={4} saturation={0} fade speed={3} />
          <Globe />
          <OrbitControls 
            enableZoom={false}
            enablePan={false}
            autoRotate
            autoRotateSpeed={0.5}
          />
        </Suspense>
      </Canvas>
    </div>
  );
} 