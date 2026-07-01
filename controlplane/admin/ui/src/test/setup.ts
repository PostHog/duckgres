// Vitest global setup: extends `expect` with jest-dom matchers (toBeInThe
// Document, toHaveTextContent, …) for component tests. Pure-function tests don't
// need it but it's harmless to load once here.
import "@testing-library/jest-dom/vitest";
