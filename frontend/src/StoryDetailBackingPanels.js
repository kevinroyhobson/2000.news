import React from 'react';
import _ from "lodash";
import useWindowDimensions from "./useWindowDimensions";


export default function StoryDetailBackingPanels(props) {

  const { clickLocation } = props;
  const { width, height } = useWindowDimensions();

  const clickDistanceFromTopEdge = clickLocation.y;
  const clickDistanceFromBottomEdge = height - clickLocation.y;
  const clickDistanceFromLeftEdge = clickLocation.x;
  const clickDistanceFromRightEdge = width - clickLocation.x;

  const wasClickOnLeftSide = clickDistanceFromLeftEdge < clickDistanceFromRightEdge;
  const wasClickAboveFold = clickDistanceFromTopEdge < clickDistanceFromBottomEdge;

  const storyDetailHorizontalTarget = width * 0.35;
  const storyDetailVerticalTarget = wasClickAboveFold
    ? 100
    : height * 0.3;

  const xIncrement = wasClickOnLeftSide
    ? (storyDetailHorizontalTarget - clickDistanceFromLeftEdge) / 8.0
    : (clickDistanceFromRightEdge - storyDetailHorizontalTarget) / 8.0;

  const yIncrement = wasClickAboveFold
    ? (storyDetailVerticalTarget - clickDistanceFromTopEdge) / 8.0
    : (clickDistanceFromBottomEdge - storyDetailVerticalTarget) / 8.0;

  const backingPanelStyles = _.map([0,1,2,3,4,5,6,7,8], function(backingPanelIndex) {
    let thisPanelStyle = {
      width: `${5 + (2.5 * backingPanelIndex)}%`,
      height: `${10 + (5 * backingPanelIndex)}%`
    }

    if (wasClickAboveFold) {
      thisPanelStyle['top'] = clickDistanceFromTopEdge + (backingPanelIndex * yIncrement);
    } else {
      thisPanelStyle['bottom'] = clickDistanceFromBottomEdge - (backingPanelIndex * yIncrement);
    }

    if (wasClickOnLeftSide) {
      thisPanelStyle['left'] = clickDistanceFromLeftEdge + (backingPanelIndex * xIncrement);
    } else {
      thisPanelStyle['right'] = clickDistanceFromRightEdge - (backingPanelIndex * xIncrement);
    }

    return thisPanelStyle;
  });

  return (
    <div className='story-detail-backing-panels'>
      {_.map(backingPanelStyles, (backingPanelStyle) => {
        return (
          <div className='story-detail-backing' style={backingPanelStyle}>
            &nbsp;
          </div>
        );
      })}
    </div>
  );
}
