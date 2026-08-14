import { formatMessageBody } from './message-model';

interface MessageBodyProps {
  body: string;
}

export default function MessageBody({ body }: MessageBodyProps) {
  return (
    <pre className="message-body-block" aria-label="Message body">
      {formatMessageBody(body) || 'The API returned an empty message body.'}
    </pre>
  );
}
