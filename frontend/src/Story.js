import React, {useState, useEffect} from "react";
import _ from 'lodash';

import Line from './Line';
import {useRecoilValue} from "recoil";
import isDebugModeState from "./state/isDebugModeState";
import getStoryTitleDisplay from "./getStoryTitleDisplay";


export default function Story(props) {

  const {story, isHeadline, onClick} = props;

  const [lines, setLines] = useState([]);
  const isDebugMode = useRecoilValue(isDebugModeState);

  useEffect(() => {
    let isNextLineAStart = true;
    let isNextLineAnEnd = false;
    let linesToSet = [];
    for (let i = 0; i < 50; i++) {
      linesToSet.push({
        isParagraphStart: isNextLineAStart,
        marginRight: isNextLineAnEnd ? `${Math.random() * 75}%` : 0,
        lineNumber: i
      });

      isNextLineAStart = isNextLineAnEnd;
      isNextLineAnEnd = Math.random() < 0.2;
    }

    setLines(linesToSet);
  }, [])

  return (
    <div className='story' onClick={onClick}>

      {isHeadline && story.ImageUrl &&
        <img className='headline-image'
             src={story.ImageUrl}
             alt={story.Headline}/>
      }

      {!isHeadline &&
        <div className='title'>{getStoryTitleDisplay(story, isDebugMode)}</div>
      }

      {_.map(lines, (line) => <Line isParagraphStart={line.isParagraphStart}
                                    marginRight={line.marginRight}
                                    lineNumber={line.lineNumber}
                                    key={line.lineNumber}/>)
      }
    </div>
  );
}
