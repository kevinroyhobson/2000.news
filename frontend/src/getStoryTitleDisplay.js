const getStoryTitleDisplay = (story, isDebugMode) => {
  return isDebugMode
           ? `${story.OriginalTitle} (${story.Source})`
           : story.Title;
};

export default getStoryTitleDisplay;
