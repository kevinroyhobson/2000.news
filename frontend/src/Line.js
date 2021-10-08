import classNames from 'classnames';


export default function Line(props) {

  const {isParagraphStart, marginRight, lineNumber} = props;

  return (
    <div className={classNames('line',
                              {'line-extended': lineNumber > 6},
                              {'paragraph-start': isParagraphStart})}
         style={{marginRight: marginRight}}>
      &nbsp;
    </div>
  );
}
