const getStoryTitleDisplay = (story, isDebugMode) => {
  if (isDebugMode && story.IsMashup) {
    return story.SourceStories.map((source) => `${source.Title} (${source.Source})`).join(' + ');
  }

  if (isDebugMode) {
    return `${story.OriginalHeadline} (${story.Source})`;
  }

  if (story.ShowOriginal) {
    return story.OriginalHeadline;
  }

  return story.Headline;
};

export default getStoryTitleDisplay;
