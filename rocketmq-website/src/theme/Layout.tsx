/**
 * Layout wrapper to inject DevWarningBanner
 * Extends the default Docusaurus layout
 */

import Layout from '@theme-original/Layout';
import DevWarningBanner from '@site/src/components/DevWarningBanner';

export default function LayoutWrapper(props: any): React.JSX.Element {
  return (
    <>
      <DevWarningBanner />
      <Layout {...props} />
    </>
  );
}
