# GitHub Pages Deployment Guide

This guide explains how to deploy the RocketMQ-Rust website to GitHub Pages using GitHub Actions with a custom domain.

## Prerequisites

1. Repository must be on GitHub: `https://github.com/mxsm/rocketmq-rust`
2. GitHub Actions must be enabled for the repository
3. Custom domain: `rocketmqrust.com`

## Configuration

### 1. GitHub Pages Settings

1. Go to your repository on GitHub
2. Navigate to **Settings** > **Pages**
3. Under **Source**, select **GitHub Actions** (not Deploy from a branch)
4. Under **Custom domain**, enter: `rocketmqrust.com`
5. Save the settings

### 2. DNS Configuration

Configure your DNS records to point to GitHub Pages. For a root domain (without www), use A records:

```
Type: A
Name: @
Values:
  - 185.199.108.153
  - 185.199.109.153
  - 185.199.110.153
  - 185.199.111.153
```

If you also want `www` to work, add a CNAME:

```
Type: CNAME
Name: www
Target: rocketmqrust.com
```

### 3. Repository Configuration

The following settings are already configured in `docusaurus.config.ts`:

```typescript
url: 'https://rocketmqrust.com',
baseUrl: '/',
organizationName: 'mxsm',
projectName: 'rocketmq-rust',
deploymentBranch: 'gh-pages',
```

### 4. CNAME File

A `CNAME` file has been created in the `static/` directory with the content:
```
rocketmqrust.com
```

GitHub Pages will automatically use this to configure your custom domain.

## Deployment Workflow

The GitHub Actions workflow is defined in `.github/workflows/deploy.yml` in the repository root and will:

1. **Trigger automatically** when you push to the `main` or `master` branch **only if files in the `rocketmq-website/` directory change**
2. **Build** the Docusaurus website
3. **Deploy** to GitHub Pages

**Note**: Changes to other parts of the repository (e.g., Rust code in the root directory) will not trigger a website deployment. Only changes within `rocketmq-website/` or the deployment workflow file itself will trigger deployment.

### Manual Deployment

You can also trigger a deployment manually:

1. Go to the **Actions** tab in your GitHub repository
2. Select **Deploy to GitHub Pages** workflow
3. Click **Run workflow**
4. Select the branch and click **Run workflow**

## Workflow Details

The deployment workflow consists of two jobs:

### Build Job

- Checks out the repository code
- Sets up Node.js 18
- Installs dependencies using `npm ci`
- Builds the website using `npm run build`
- Uploads the build artifacts

### Deploy Job

- Deploys the uploaded artifacts to GitHub Pages
- Runs only after the build job succeeds
- Uses the `github-pages` environment

## Accessing Your Website

After successful deployment and DNS configuration, your website will be available at:

```
https://rocketmqrust.com
```

During development or before DNS propagation, you can also access via GitHub Pages default URL:

```
https://mxsm.github.io/rocketmq-rust/
```

## Troubleshooting

### Deployment Fails

1. Check the **Actions** tab for detailed error logs
2. Ensure GitHub Pages is enabled in repository settings
3. Verify that the workflow file has correct permissions

### Build Errors

1. Check that all dependencies are correctly specified in `package.json`
2. Ensure the build works locally with `npm run build`
3. Review the build logs in the Actions tab

### Website Not Updating

1. Check if the deployment workflow completed successfully
2. Clear your browser cache
3. GitHub Pages may take a few minutes to update

### Custom Domain Not Working

1. **DNS Propagation**: DNS changes can take up to 24-48 hours to propagate worldwide
2. **Check DNS Status**: Use tools like `dig rocketmqrust.com` or `nslookup rocketmqrust.com` to verify DNS
3. **GitHub Pages Status**: Ensure the custom domain is saved in **Settings** > **Pages**
4. **HTTPS Status**: GitHub Pages will automatically provision SSL certificates for your custom domain
5. **Check CNAME**: Ensure the `CNAME` file exists in the `static/` directory
6. **DNS Configuration**: Verify your DNS records match the configuration above

### HTTPS Certificate Issues

GitHub Pages automatically provisions Let's Encrypt SSL certificates for custom domains. If you see certificate errors:

1. Wait a few minutes after setting up the custom domain
2. Check **Settings** > **Pages** for certificate status
3. Ensure DNS is correctly configured
4. If issues persist, remove and re-add the custom domain in GitHub Pages settings

### Permissions Errors

The workflow includes the necessary permissions:

```yaml
permissions:
  contents: read
  pages: write
  id-token: write
```

If you encounter permission errors, ensure these are set correctly in the workflow file.

## Local Testing

Before pushing changes, you can test the build locally:

```bash
cd rocketmq-website
npm install
npm run build
npm run serve
```

This will serve the production build at `http://localhost:3000`

## Customization

### Changing Deployment Branch

If you want to deploy from a different branch, modify `.github/workflows/deploy.yml`:

```yaml
on:
  push:
    branches:
      - your-branch-name
```

### Changing Node.js Version

To use a different Node.js version, modify the workflow:

```yaml
- name: Setup Node.js
  uses: actions/setup-node@v4
  with:
    node-version: 20  # Change to your preferred version
```

### Adding Environment Variables

To add environment variables for the build:

```yaml
- name: Build website
  working-directory: ./rocketmq-website
  run: npm run build
  env:
    YOUR_VARIABLE: value
```

## Related Documentation

- [GitHub Pages Documentation](https://docs.github.com/en/pages)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Docusaurus Deployment Guide](https://docusaurus.io/docs/deploying)
