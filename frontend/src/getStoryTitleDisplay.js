const getStoryTitleDisplay = (story, isDebugMode) => {
  if (isDebugMode) {
    return `${story.OriginalHeadline} (${story.Source})`;
  }

  if (story.ShowOriginal) {
    return story.OriginalHeadline;
  }

  return story.Headline;
};

export default getStoryTitleDisplay;
