import React from 'react';

import Box from '@mui/material/Box';

import StoryDetailBackingPanels from "./StoryDetailBackingPanels";
import {useRecoilValue} from "recoil";
import isDebugModeState from "./state/isDebugModeState";
import getStoryTitleDisplay from "./getStoryTitleDisplay";
import CopyStoryLink from "./CopyStoryLink";


export default function StoryDetail(props) {

  const {story, onClick, clickLocation} = props;

  const isDebugMode = useRecoilValue(isDebugModeState);

  const siblingHeadlines = story.SiblingHeadlines || [];

  return (
    <div onClick={onClick}>

      <StoryDetailBackingPanels clickLocation={clickLocation}/>

      <div className='story-detail'>
        <Box mb={2} className='title'>
          {getStoryTitleDisplay(story, isDebugMode)}
          <CopyStoryLink story={story}/>
        </Box>

        <Box className='content'>
          {story.Description}
        </Box>

        {story.ImageUrl &&
          <Box mt={2}>
            <img src={story.ImageUrl}
                 alt={getStoryTitleDisplay(story, isDebugMode)}/>
          </Box>
        }

        {isDebugMode && siblingHeadlines.length > 0 &&
          <Box mt={3} className='sibling-headlines'>
            <Box className='sibling-headlines-header'>Other headline options:</Box>
            {siblingHeadlines.map((sibling) => (
              <Box key={sibling.HeadlineId} className='sibling-headline'>
                <a
                  href={`/${story.YearMonthDay}/${sibling.HeadlineId}`}
                  onClick={(e) => e.stopPropagation()}
                >
                  {sibling.Headline}
                </a>
                {sibling.Angle &&
                  <span className='sibling-angle'> [{sibling.Angle}]</span>
                }
              </Box>
            ))}
          </Box>
        }

      </div>

    </div>
  );
}
