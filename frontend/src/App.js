import React from 'react';
import ReactGA from 'react-ga';
import { RecoilRoot } from 'recoil';

import './App.css';
import Newspaper from './Newspaper';


function App() {

  ReactGA.initialize('UA-27999547-7');
  ReactGA.pageview('/');

  return (
    <RecoilRoot>
      <div className='nyc-background'>
        <Newspaper />
        <div className='credit'>
          <a href='http://kevinhobson.com/' target='_blank'>kevinhobson.com</a>
        </div>
      </div>
    </RecoilRoot>
  );
}

export default App;
