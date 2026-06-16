import React, { useEffect } from 'react';
import ReactGA from 'react-ga4';

import './App.css';
import Newspaper from './Newspaper';
import { DebugModeProvider } from './state/DebugModeContext';

// Set VITE_GA_MEASUREMENT_ID in .env to enable Google Analytics (GA4).
const GA_MEASUREMENT_ID = import.meta.env.VITE_GA_MEASUREMENT_ID;
if (GA_MEASUREMENT_ID) {
  ReactGA.initialize(GA_MEASUREMENT_ID);
}

function App() {
  useEffect(() => {
    if (GA_MEASUREMENT_ID) {
      ReactGA.send({ hitType: 'pageview', page: window.location.pathname });
    }
  }, []);

  return (
    <DebugModeProvider>
      <div className='nyc-background'>
        <Newspaper />
        <div className='credit'>
          <a href='http://kevinhobson.com/' target='_blank' rel='noreferrer'>kevinhobson.com</a>
        </div>
      </div>
    </DebugModeProvider>
  );
}

export default App;
