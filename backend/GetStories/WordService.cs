using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace GetStories;

public class WordService
{
    private readonly Dictionary<WordType, HashSet<string>> _wordSetByWordType = new Dictionary<WordType, HashSet<string>>();
	private readonly Dictionary<WordType, string[]> _wordsByWordType = new Dictionary<WordType, string[]>();

    public bool IsWordOfType(string word, WordType type)
    {
        if (!_wordSetByWordType.ContainsKey(type))
        {
            LoadWordsOfType(type);
        }

        var setToCheck = _wordSetByWordType[type];
        return setToCheck.Contains(word.ToLower());
    }

    public string GetRandomWordOfType(WordType type)
    {
        if (!_wordSetByWordType.ContainsKey(type))
        {
            LoadWordsOfType(type);
        }

        var arrayToGetFrom = _wordsByWordType[type];
        int index = new Random().Next(arrayToGetFrom.Length);
        return arrayToGetFrom[index];
    }

    private void LoadWordsOfType(WordType type)
    {
        var filePath = _filePathByWordType[type];
		_wordsByWordType[type] = File.ReadAllLines(filePath)
                					 .Select(word => word.ToLower())
									 .ToArray();
        _wordSetByWordType[type] = _wordsByWordType[type].ToHashSet();

        if (type == WordType.Person)
        {
			_wordsByWordType[WordType.LastName] = _wordsByWordType[WordType.Person]
													  .Select(line => line.Split().Last())
													  .ToArray();
            _wordSetByWordType[WordType.LastName] = _wordsByWordType[WordType.LastName]
                                                        .ToHashSet();
        }
    }

    private Dictionary<WordType, string> _filePathByWordType = new Dictionary<WordType, string>()
    {
        {WordType.Noun, "words/nouns.txt"},
        {WordType.Adjective, "words/adjectives.txt"},
        {WordType.NewspaperName, "words/newspaper-names.txt"},
        {WordType.Place, "words/places.txt"},
        {WordType.Number, "words/numbers.txt"},
        {WordType.WrittenNumber, "words/written-numbers.txt"},
        {WordType.DayOfWeek, "words/days-of-week.txt"},
        {WordType.Month, "words/months.txt"},
        {WordType.Person, "words/people.txt"},
    };
    
}

public enum WordType
{
    Noun,
    Adjective,
    NewspaperName,
    Place,
    Number,
    WrittenNumber,
    DayOfWeek,
    Month,
    Person,
    LastName
}
