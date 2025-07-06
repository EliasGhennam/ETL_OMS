import { render, screen } from '@testing-library/react';
import Home from '../app/page';

// Mock the components that might cause issues in tests
jest.mock('../components/Hero3D', () => {
  return function MockHero3D() {
    return <div data-testid="hero-3d">Hero 3D Component</div>;
  };
});

jest.mock('../components/AnimatedText', () => {
  return function MockAnimatedText({ children }: { children: React.ReactNode }) {
    return <div data-testid="animated-text">{children}</div>;
  };
});

describe('Home Page', () => {
  it('renders without crashing', () => {
    render(<Home />);
    expect(screen.getByTestId('hero-3d')).toBeInTheDocument();
  });

  it('displays animated text', () => {
    render(<Home />);
    expect(screen.getByTestId('animated-text')).toBeInTheDocument();
  });
}); 