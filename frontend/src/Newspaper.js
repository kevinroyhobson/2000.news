import React, { useEffect, useState } from 'react';
import ReactGA from 'react-ga';
import { DateTime } from 'luxon';
import axios from 'axios';
import classNames from 'classnames';
import _ from 'lodash';

import Grid from '@mui/material/Grid';
import Box from '@mui/material/Box';

import './Newspaper.css'
import Story from './Story';
import StoryDetail from "./StoryDetail";


export default function Newspaper() {

  const [paperName, setPaperName] = useState([]);
  const [stories, setStories] = useState([]);
  const [storyOrder, setStoryOrder] = useState([1, 2, 3, 4]);
  const [selectedStory, setSelectedStory] = useState(null);
  const [selectedStoryClickLocation, setSelectedStoryClickLocation] = useState(null);

  function loadPaper() {
    axios({
      method: 'get',
      url: 'https://api.2000.news/stories/'
    })
      .then(function (response) {
        setPaperName(response.data.PaperName);
        setStories(response.data.Stories);
        setStoryOrder(_.shuffle(storyOrder));
      });
  }

  useEffect(() => {
    loadPaper();
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
    if (story !== null &&
        selectedStory !== null &&
        selectedStory.Title === story.Title) {
      story = null;
    }

    setSelectedStory(story);
    setSelectedStoryClickLocation({x: clickEvent.clientX,
                                   y: clickEvent.clientY});

    ReactGA.event({category: 'newspaper', action: 'view-story-detail'})
  }

  function handleClosePaper() {
    setStories([]);
    setSelectedStory(null);
    loadPaper();

    ReactGA.event({category: 'newspaper', action: 'close-and-reload'})
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
        <Box className='edition'>
          {getEditionForDate(date)} Edition
        </Box>
      </Box>

      <Box mt={3} mb={2} className='headline' onClick={(e) => handleStoryDetailOpen(e, stories[0])}>
        {stories[0].Title}
      </Box>

      <Box className='stories'>

        <Grid container spacing={2}>

          <Grid item xs={12} md={2} order={{xs: 2, md: storyOrder[0]}}>
            <Story story={stories[1]}
                   onClick={(e) => handleStoryDetailOpen(e, stories[1])} />
          </Grid>

          <Grid item xs={12} md={4} order={{xs: 1, md: storyOrder[1]}}>
            <Story story={stories[0]}
                   isHeadline={true}
                   onClick={(e) => handleStoryDetailOpen(e, stories[0])} />
          </Grid>

          <Grid item xs={12} md={4} order={{xs: 3, md: storyOrder[2]}}>
            <Story story={stories[2]}
                   onClick={(e) => handleStoryDetailOpen(e, stories[2])} />
          </Grid>

          <Grid item xs={12} md={2} order={{xs: 4, md: storyOrder[3]}}>
            <Story story={stories[3]}
                   onClick={(e) => handleStoryDetailOpen(e, stories[3])} />
          </Grid>

        </Grid>

      </Box>

      {selectedStory !== null &&
      <StoryDetail story={selectedStory}
                   onClick={(e) => handleStoryDetailOpen(e, null)}
                   clickLocation={selectedStoryClickLocation} />
      }

    </div>
  );
};
