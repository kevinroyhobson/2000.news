import React from 'react';
import ReactGA from 'react-ga';

import './App.css';
import Newspaper from './Newspaper';


function App() {

  ReactGA.initialize('UA-27999547-7');
  ReactGA.pageview('/');

  return (
    <div className='nyc-background'>
      <Newspaper />
      <div className='credit'>
        <a href='http://kevinhobson.com/' target='_blank'>kevinhobson.com</a>
      </div>
    </div>
  );
}

export default App;
