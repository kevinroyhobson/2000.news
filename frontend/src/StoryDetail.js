import React from 'react';

import Box from '@mui/material/Box';

import StoryDetailBackingPanels from "./StoryDetailBackingPanels";


export default function StoryDetail(props) {

  const { story, onClick, clickLocation } = props;

  return (
    <div onClick={onClick}>

      <StoryDetailBackingPanels clickLocation={clickLocation} />

      <div className='story-detail'>
        <Box mb={2} className='title'>
          {story.Title}
        </Box>

        <Box className='content'>
          {story.Description}
        </Box>

        {story.ImageUrl &&
            <Box mt={2}>
              <img src={story.ImageUrl}
                   alt={story.Title}/>
            </Box>
        }

        <Box className='content'>
          {story.Content}
        </Box>

      </div>

    </div>
  );
}
