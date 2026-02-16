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

  const siblingHeadlines = (story.SiblingHeadlines || [])
    .slice()
    .sort((a, b) => (a.Rank ?? Infinity) - (b.Rank ?? Infinity));

  return (
    <div onClick={onClick}>

      <StoryDetailBackingPanels clickLocation={clickLocation}/>

      <div className='story-detail'>
        <Box mb={2} className='title'>
          {getStoryTitleDisplay(story, isDebugMode)}
          <CopyStoryLink story={story}/>
        </Box>

        {!isDebugMode &&
          <Box className='content'>
            {story.Description}
          </Box>
        }

        {!isDebugMode && story.ImageUrl &&
          <Box mt={2}>
            <img src={story.ImageUrl}
                 alt={getStoryTitleDisplay(story, isDebugMode)}/>
          </Box>
        }

        {isDebugMode && siblingHeadlines.length > 0 &&
          <Box mt={3} className='headline-list'>
            <Box className='headline-list-header'>Other headline options:</Box>
            {siblingHeadlines.map((sibling) => (
              <Box key={sibling.HeadlineId} className='headline-list-item'>
                <a
                  href={`/${story.YearMonthDay}/${sibling.HeadlineId}`}
                  onClick={(e) => e.stopPropagation()}
                >
                  {sibling.Headline}
                </a>
                {(sibling.Angle || sibling.Rank != null) &&
                  <span className='headline-angle'> [{[sibling.Angle, sibling.Rank != null ? `rank ${sibling.Rank}` : ''].filter(Boolean).join(', ')}]</span>
                }
              </Box>
            ))}
          </Box>
        }

      </div>

    </div>
  );
}
