import React, {useEffect, useState} from 'react';
import {useRecoilState} from 'recoil';
import ReactGA from 'react-ga';
import {DateTime} from 'luxon';
import axios from 'axios';
import classNames from 'classnames';

import Grid from '@mui/material/Grid';
import Box from '@mui/material/Box';

import './Newspaper.css'
import Story from './Story';
import StoryDetail from './StoryDetail';
import isDebugModeState from './state/isDebugModeState';
import getStoryTitleDisplay from './getStoryTitleDisplay';


export default function Newspaper() {

  const [paperName, setPaperName] = useState([]);
  const [stories, setStories] = useState([]);
  const [selectedStory, setSelectedStory] = useState(null);
  const [selectedStoryClickLocation, setSelectedStoryClickLocation] = useState(null);
  const [isDebugMode, setIsDebugMode] = useRecoilState(isDebugModeState);

  function loadPaper() {
    const defaultApiPath = '/today';
    let path = window.location.pathname.length > 1 ? window.location.pathname : defaultApiPath;
    loadFromPath(path)
      .catch(function () {
        loadFromPath(defaultApiPath);
      });
  }

  function loadFromPath(path) {
    const request = axios.get(`https://api.2000.news${path}${isDebugMode ? '?debug=true' : ''}`);
    request.then(function (response) {
      setPaperName(response.data.PaperName);
      setStories(response.data.Stories);
    });

    return request;
  }

  useEffect(() => {
    loadPaper();

    function handleKeyDown(event) {
      if (event.key === 'd') {
        handleToggleDebugMode();
      }
    }

    document.addEventListener('keydown', handleKeyDown);

    return function cleanup() {
      document.removeEventListener('keydown', handleKeyDown);
    };

  }, []);

  function getEditionForDate(date) {

    if (date.hour < 6) {
      return 'Early'
    }

    if (date.hour < 12) {
      return 'Morning'
    }

    if (date.hour < 18) {
      return 'Afternoon';
    }

    return 'Evening';
  }

  if (stories.length === 0) {
    return <div/>
  }

  function handleStoryDetailOpen(clickEvent, story) {
    if (story !== null && selectedStory !== null) {
      story = null;
    }

    setSelectedStory(story);
    setSelectedStoryClickLocation({
      x: clickEvent.clientX,
      y: clickEvent.clientY
    });

    ReactGA.event({category: 'newspaper', action: 'view-story-detail'})
  }

  function handleClosePaper() {
    setStories([]);
    setSelectedStory(null);
    window.history.pushState({page: 1}, "2000.news", "/");
    loadPaper();

    ReactGA.event({category: 'newspaper', action: 'close-and-reload'})
  }

  function handleToggleDebugMode() {
    ReactGA.event({category: 'newspaper', action: 'toggle-debug-mode'});
    setIsDebugMode(currentValue => !currentValue);
  }

  const date = DateTime.now();

  return (
    <div className={classNames('newspaper', {'closed': stories.length === 0})}>

      <div className='close-box' onClick={() => handleClosePaper()}>
      </div>

      <Box className='paper-name'>
        {paperName}
      </Box>

      <Box className='paper-details'>
        <Box className='date'>
          <span className='long-date'>
            {date.toFormat('EEEE d, MMMM y')}
          </span>
          <span className='short-date'>
            {date.toLocaleString(DateTime.DATE_MED)}
          </span>
        </Box>
        <Box className='edition' onClick={handleToggleDebugMode}>
          {getEditionForDate(date)} Edition
        </Box>
      </Box>

      <Box mt={3} mb={2} className='headline' onClick={(e) => handleStoryDetailOpen(e, stories[0])}>
        {getStoryTitleDisplay(stories[0], isDebugMode)}
      </Box>

      <Box className='stories'>

        <Grid container spacing={2}>

          <Grid item xs={12} md={2} order={{xs: 2, md: 1}}>
            <Story story={stories[1]}
                   onClick={(e) => handleStoryDetailOpen(e, stories[1])}/>
          </Grid>

          <Grid item xs={12} md={4} order={{xs: 1, md: 2}}>
            <Story story={stories[0]}
                   isHeadline={true}
                   onClick={(e) => handleStoryDetailOpen(e, stories[0])}/>
          </Grid>

          <Grid item xs={12} md={4} order={3}>
            <Story story={stories[2]}
                   onClick={(e) => handleStoryDetailOpen(e, stories[2])}/>
          </Grid>

          <Grid item xs={12} md={2} order={4}>
            <Story story={stories[3]}
                   onClick={(e) => handleStoryDetailOpen(e, stories[3])}/>
          </Grid>

        </Grid>

      </Box>

      {selectedStory !== null &&
        <StoryDetail story={selectedStory}
                     onClick={(e) => handleStoryDetailOpen(e, null)}
                     clickLocation={selectedStoryClickLocation}/>
      }

    </div>
  );
};
