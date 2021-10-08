using System;
using System.Collections.Generic;
using System.Linq;

namespace News2000
{
    public class SubversionService
    {
        private static readonly double SubversionRate = double.Parse(Environment.GetEnvironmentVariable("SubversionRate") ?? "0.0");
        private static readonly int MaxSubstitutionsPerStory = int.Parse(Environment.GetEnvironmentVariable("MaxSubstitutionsPerStory") ?? "0");

        private static readonly HashSet<WordType> ExemptWordTypes = new HashSet<WordType>() {WordType.NewspaperName};
        private static readonly IEnumerable<WordType> WordTypesToAlter = ((WordType[]) Enum.GetValues(typeof(WordType)))
                                                                             .Where(type => !ExemptWordTypes.Contains(type));

        private readonly WordService _wordService = new WordService();
        private readonly Random _random = new Random();
        
        public string GetNewspaperName()
        {
            return $"The {Capitalize(_wordService.GetRandomWordOfType(WordType.Adjective))} {Capitalize(_wordService.GetRandomWordOfType(WordType.NewspaperName))}";
        }

        public Story SubvertStory(Story story)
        {
            if (_random.NextDouble() > SubversionRate)
            {
                return story;
            }
            
            var candidateWordsToAlter = GetCandidateWordsToAlter(story);
            var newWordByOldWord = GetReplacementWords(candidateWordsToAlter);
            return ApplyWordSubstitutionsToStory(story, newWordByOldWord);
        }

        private List<string> GetCandidateWordsToAlter(Story story)
        {
            var candidateWordsToAlter = new List<String>();
            
            var titleTokens = story.Title.Split(new [] {' ', ',', '.', '\'', '‘', '’', '"', '“', '”', '$', '-'}, 
                                                StringSplitOptions.RemoveEmptyEntries);
            candidateWordsToAlter.AddRange(titleTokens);
            
            // For each word, if it is possibly a plural form of a word, remove the s to check if the possible singular
            // form is a word.
            foreach (var word in titleTokens.Where(token => token.Length >= 3 && token[^1] == 's'))
            {
                candidateWordsToAlter.Add(word[..^1]);
            }

            // Also check if each combination of two consecutive words together are considered a word. This is useful
            // for full names.
            for (int i = 0; i < titleTokens.Length - 1; i++)
            {
                candidateWordsToAlter.Add($"{titleTokens[i]} {titleTokens[i+1]}");
            }

            return candidateWordsToAlter;
        }

        private Dictionary<string, string> GetReplacementWords(IEnumerable<string> candidateWords)
        {
            var newWordByOldWord = new Dictionary<string, string>();
            
            foreach (var word in candidateWords)
            {
                string replacementWord = null;

                foreach (WordType wordType in WordTypesToAlter)
                {
                    if (_wordService.IsWordOfType(word, wordType))
                    {
                        replacementWord = _wordService.GetRandomWordOfType(wordType);
                    }
                }

                if (replacementWord != null)
                {
                    replacementWord = MatchCapitalization(replacementWord, word);
                    newWordByOldWord[word] = replacementWord;
                }
            }

            return newWordByOldWord;
        }

        private Story ApplyWordSubstitutionsToStory(Story story, Dictionary<string, string> newWordByOldWord)
        {
            int maxSubstitutionsForThisStory = _random.Next(1, MaxSubstitutionsPerStory + 1);
            int numSubstitutionsPerformed = 0;
            
            // If there are multiple-word substitutions (which contain a space), perform those first.
            foreach (var (oldPhrase, newPhrase) in newWordByOldWord.OrderByDescending(change => change.Key.Contains(" "))
                                                                   .ThenBy(ch => Guid.NewGuid()))
            {
                story.Title = story.Title.Replace(oldPhrase, newPhrase);
                story.Description = story.Description.Replace(oldPhrase, newPhrase);
                story.Content = story.Content.Replace(oldPhrase, newPhrase);
                
                numSubstitutionsPerformed++;

                if (numSubstitutionsPerformed >= maxSubstitutionsForThisStory)
                {
                    break;
                }
            }

            return story;
        }

        private string MatchCapitalization(string stringToChange, string stringToMatch)
        {
            return IsCapitalized(stringToMatch) 
                ? Capitalize(stringToChange) 
                : stringToChange.ToLower();
        }
        
        private string Capitalize(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return str;
            }

            var capitalizedTokens = str.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                                       .Select(token => $"{char.ToUpper(token[0])}{token[1..]}");

            return string.Join(' ', capitalizedTokens);
        }

        private bool IsCapitalized(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return false;
            }

            return str[0] >= 'A' && str[0] <= 'Z';
        }
    }
}
