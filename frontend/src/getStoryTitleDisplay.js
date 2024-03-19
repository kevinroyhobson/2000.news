const getStoryTitleDisplay = (story, isDebugMode) => {
  return isDebugMode
    ? `${story.OriginalHeadline} (${story.Source})`
    : story.Headline;
};

export default getStoryTitleDisplay;
