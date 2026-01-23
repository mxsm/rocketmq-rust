/**
 * Ballpit Dynamic Background Effect
 * Modified for RocketMQ-Rust Documentation Site
 *
 * Based on: https://reactbits.dev/backgrounds/ballpit
 * License: MIT + Commons Clause
 *
 * @version 1.0.0
 * @author RocketMQ Rust Team
 */

(function() {
  'use strict';

  // ============================================
  // 配置参数 (可通过页面配置覆盖)
  // ============================================
  const CONFIG = {
    // 球体数量
    ballCount: 25,
    // 最小/最大半径
    minRadius: 15,
    maxRadius: 50,
    // 最小/最大速度
    minSpeed: 0.3,
    maxSpeed: 1.5,
    // 颜色主题 (RocketMQ Rust 主题色)
    colors: ['#FF6B35', '#004E89', '#F7C531', '#1A535C', '#FF6B6B'],
    // 鼠标交互半径
    mouseInteractionRadius: 150,
    // 是否在移动端启用
    enableOnMobile: false,
    // 移动端断点
    mobileBreakpoint: 768
  };

  // ============================================
  // 工具函数
  // ============================================
  const utils = {
    random(min, max) {
      return Math.random() * (max - min) + min;
    },

    // 检测是否为移动设备
    isMobile() {
      return window.innerWidth < CONFIG.mobileBreakpoint;
    },

    // 检测设备性能（简单版）
    isLowPerformance() {
      // 可以添加更复杂的性能检测逻辑
      return false;
    }
  };

  // ============================================
  // Ball 类：单个球体
  // ============================================
  class Ball {
    constructor(canvas, config) {
      this.canvas = canvas;
      this.radius = utils.random(config.minRadius, config.maxRadius);
      this.x = utils.random(this.radius, canvas.width - this.radius);
      this.y = utils.random(this.radius, canvas.height - this.radius);
      this.dx = utils.random(-config.maxSpeed, config.maxSpeed);
      this.dy = utils.random(-config.maxSpeed, config.maxSpeed);
      this.color = config.colors[Math.floor(utils.random(0, config.colors.length))];
      this.originalRadius = this.radius;
    }

    update(mouse) {
      // 边界碰撞检测
      if (this.x + this.radius > this.canvas.width || this.x - this.radius < 0) {
        this.dx = -this.dx;
      }
      if (this.y + this.radius > this.canvas.height || this.y - this.radius < 0) {
        this.dy = -this.dy;
      }

      // 鼠标交互效果
      if (mouse.x !== null) {
        const dist = Math.hypot(mouse.x - this.x, mouse.y - this.y);
        if (dist < CONFIG.mouseInteractionRadius) {
          // 靠近鼠标时变大
          if (this.radius < this.originalRadius + 20) {
            this.radius += 1;
          }
        } else if (this.radius > this.originalRadius) {
          // 恢复原始大小
          this.radius -= 1;
        }
      }

      // 更新位置
      this.x += this.dx;
      this.y += this.dy;
    }

    draw(ctx) {
      ctx.beginPath();
      ctx.arc(this.x, this.y, this.radius, 0, Math.PI * 2);
      ctx.fillStyle = this.color;
      ctx.globalAlpha = 0.6;
      ctx.fill();
      ctx.globalAlpha = 1;
      ctx.closePath();
    }
  }

  // ============================================
  // BallPit 类：主控制器
  // ============================================
  class BallPit {
    constructor(canvasElement) {
      this.canvas = canvasElement;
      if (!this.canvas) {
        console.warn('[Ballpit] Canvas element not found');
        return;
      }

      // 检查是否应该启用
      if (!this.shouldEnable()) {
        console.log('[Ballpit] Disabled on this device');
        this.canvas.style.display = 'none';
        return;
      }

      this.ctx = this.canvas.getContext('2d');
      this.balls = [];
      this.mouse = { x: null, y: null };
      this.animationId = null;

      this.init();
    }

    /**
     * 检查是否应该启用 Ballpit 效果
     */
    shouldEnable() {
      // 移动端检测
      if (!CONFIG.enableOnMobile && utils.isMobile()) {
        return false;
      }

      // 性能检测
      if (utils.isLowPerformance()) {
        return false;
      }

      // 检查 Canvas 支持
      if (!this.canvas.getContext) {
        return false;
      }

      return true;
    }

    /**
     * 初始化
     */
    init() {
      // 延迟初始化，确保 DOM 完全渲染
      setTimeout(() => {
        this.resize();
        console.log('[Ballpit] Canvas size:', this.canvas.width, 'x', this.canvas.height);

        if (this.canvas.width === 0 || this.canvas.height === 0) {
          console.warn('[Ballpit] Canvas has zero size, retrying...');
          setTimeout(() => {
            this.resize();
            this.createBalls();
            this.bindEvents();
            this.start();
            console.log('[Ballpit] Initialized successfully (retry)');
          }, 100);
          return;
        }

        this.createBalls();
        this.bindEvents();
        this.start();

        console.log('[Ballpit] Initialized successfully with', this.balls.length, 'balls');
      }, 50);
    }

    /**
     * 创建球体
     */
    createBalls() {
      // 根据屏幕大小调整球体数量
      const count = utils.isMobile() ? Math.floor(CONFIG.ballCount / 2) : CONFIG.ballCount;

      for (let i = 0; i < count; i++) {
        this.balls.push(new Ball(this.canvas, CONFIG));
      }
    }

    /**
     * 动画循环
     */
    animate() {
      this.animationId = requestAnimationFrame(() => this.animate());

      // 清空画布
      this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);

      // 更新并绘制所有球体
      this.balls.forEach(ball => {
        ball.update(this.mouse);
        ball.draw(this.ctx);
      });
    }

    /**
     * 启动动画
     */
    start() {
      if (!this.animationId) {
        this.animate();
      }
    }

    /**
     * 停止动画
     */
    stop() {
      if (this.animationId) {
        cancelAnimationFrame(this.animationId);
        this.animationId = null;
      }
    }

    /**
     * 调整画布大小
     */
    resize() {
      const container = this.canvas.parentElement;
      if (container) {
        this.canvas.width = container.offsetWidth;
        this.canvas.height = container.offsetHeight;
      } else {
        this.canvas.width = window.innerWidth;
        this.canvas.height = window.innerHeight;
      }
    }

    /**
     * 绑定事件监听
     */
    bindEvents() {
      // 窗口大小改变
      let resizeTimeout;
      window.addEventListener('resize', () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => {
          this.resize();
          // 重新创建球体以适应新尺寸
          this.balls = [];
          this.createBalls();
        }, 250);
      });

      // 鼠标移动
      this.canvas.addEventListener('mousemove', (e) => {
        const rect = this.canvas.getBoundingClientRect();
        this.mouse.x = e.clientX - rect.left;
        this.mouse.y = e.clientY - rect.top;
      });

      // 鼠标离开
      this.canvas.addEventListener('mouseleave', () => {
        this.mouse.x = null;
        this.mouse.y = null;
      });

      // 触摸事件（移动端）
      this.canvas.addEventListener('touchmove', (e) => {
        const rect = this.canvas.getBoundingClientRect();
        this.mouse.x = e.touches[0].clientX - rect.left;
        this.mouse.y = e.touches[0].clientY - rect.top;
      }, { passive: true });

      this.canvas.addEventListener('touchend', () => {
        this.mouse.x = null;
        this.mouse.y = null;
      }, { passive: true });

      // 页面可见性变化（性能优化）
      document.addEventListener('visibilitychange', () => {
        if (document.hidden) {
          this.stop();
        } else {
          this.start();
        }
      });
    }
  }

  // ============================================
  // 初始化
  // ============================================
  function init() {
    const canvas = document.getElementById('ballpit-canvas');
    if (canvas) {
      // 将实例挂载到 canvas 元素上，便于调试
      canvas.ballpitInstance = new BallPit(canvas);
    }
  }

  // DOM 加载完成后初始化
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // 导出到全局（可选，用于调试）
  window.BallPit = BallPit;

})();
