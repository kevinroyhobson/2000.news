import { createContext, useContext, useState } from 'react';

const DebugModeContext = createContext(null);

export function DebugModeProvider({ children }) {
  const state = useState(false);
  return (
    <DebugModeContext.Provider value={state}>
      {children}
    </DebugModeContext.Provider>
  );
}

export function useDebugMode() {
  return useContext(DebugModeContext);
}
