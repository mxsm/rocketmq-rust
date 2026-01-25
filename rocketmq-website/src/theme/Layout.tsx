/**
 * Layout wrapper to extend the default Docusaurus layout
 */

import Layout from '@theme-original/Layout';

export default function LayoutWrapper(props: any): React.JSX.Element {
  return <Layout {...props} />;
}
