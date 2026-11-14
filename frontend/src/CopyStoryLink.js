import {useState} from "react";
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import Box from "@mui/material/Box";

import {useDebugMode} from "./state/DebugModeContext";

function getStoryLink(story, isDebugMode) {
  const link = `https://www.2000.news/${story.YearMonthDay}/${story.HeadlineId}`;
  const showsRealHeadline = isDebugMode && !story.IsMashup;
  return showsRealHeadline ? `${link}?real=true` : link;
}

export default function CopyStoryLink(props) {

  const {story} = props;

  const [hasCopied, setHasCopied] = useState(false);
  const [isVisible, setIsVisible] = useState(true);
  const [isDebugMode] = useDebugMode();

  function onClick(event) {
    event.stopPropagation();
    setHasCopied(true);
    navigator.clipboard.writeText(getStoryLink(story, isDebugMode));
    setTimeout(function () {
      setIsVisible(false);
    }, 1250);
  }

  return (
    <Box display="inline" sx={{pl: 2, transitionDuration: '0.75s', opacity: isVisible ? 0.5 : 0}}>
      <Box display="inline" sx={{transitionDuration: '0.3s', opacity: hasCopied ? 0.5 : 0, zIndex: 1}}>
        <CheckIcon/>
      </Box>
      <Box display="inline" sx={{ml: -3, transitionDuration: '0.3s', opacity: hasCopied ? 0 : 0.5, zIndex: 2}}>
        <ContentCopyIcon sx={{cursor: hasCopied ? 'inherit' : 'pointer'}}
                         onClick={onClick}/>
      </Box>

    </Box>
  );
}
